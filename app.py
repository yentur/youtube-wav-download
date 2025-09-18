import asyncio
import aiohttp
import aioboto3
import yt_dlp
import os
import csv
import random
import json
import logging
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import tempfile
import shutil
from pathlib import Path
from typing import List, Dict, Tuple, Optional
import re
from dataclasses import dataclass
from contextlib import asynccontextmanager
import time

# Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('downloader.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class Config:
    """Configuration class for better organization"""
    s3_bucket: str = os.getenv("S3_BUCKET", "")
    s3_folder: str = os.getenv("S3_FOLDER", "")
    aws_access_key_id: str = os.getenv("AWS_ACCESS_KEY_ID", "")
    aws_secret_access_key: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
    aws_region: str = os.getenv("AWS_REGION", "us-east-1")
    api_base_url: str = os.getenv("API_BASE_URL", "")
    log_file: str = "download_log.csv"
    max_workers: int = 8
    max_retries: int = 3
    request_timeout: int = 30
    chunk_size: int = 8192
    
    def validate(self) -> bool:
        """Validate required configuration"""
        required_fields = [
            self.s3_bucket, self.aws_access_key_id, 
            self.aws_secret_access_key, self.api_base_url
        ]
        return all(field.strip() for field in required_fields)

class SecurityUtils:
    """Security utilities for input validation and sanitization"""
    
    @staticmethod
    def sanitize_filename(filename: str, max_length: int = 100) -> str:
        """Sanitize filename for security"""
        # Remove dangerous characters
        filename = re.sub(r'[<>:"/\\|?*\x00-\x1f]', '_', filename)
        # Remove leading/trailing dots and spaces
        filename = filename.strip('. ')
        # Limit length
        if len(filename) > max_length:
            name, ext = os.path.splitext(filename)
            filename = name[:max_length-len(ext)] + ext
        return filename or "unnamed"
    
    @staticmethod
    def validate_url(url: str) -> bool:
        """Validate URL format"""
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return bool(url_pattern.match(url))

class VideoDownloader:
    """Main video downloader class with optimizations"""
    
    def __init__(self, config: Config):
        self.config = config
        self.session = None
        self.s3_session = None
        self.temp_dir = None
        self.stats = {
            'success': 0,
            'skipped': 0,
            'errors': 0,
            'total': 0
        }
        
    async def __aenter__(self):
        """Async context manager entry"""
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=self.config.request_timeout),
            connector=aiohttp.TCPConnector(limit=100, limit_per_host=30)
        )
        self.s3_session = aioboto3.Session()
        self.temp_dir = tempfile.mkdtemp(prefix="yt_downloader_")
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    def log_to_csv(self, user: str, video_url: str, status: str, message: str = ""):
        """Thread-safe CSV logging"""
        try:
            file_exists = os.path.isfile(self.config.log_file)
            with open(self.config.log_file, "a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                if not file_exists:
                    writer.writerow(["timestamp", "user", "video_url", "status", "message"])
                writer.writerow([datetime.now().isoformat(), user, video_url, status, message])
        except Exception as e:
            logger.error(f"CSV logging error: {e}")

    async def check_s3_file_exists(self, s3_key: str) -> bool:
        """Check if file exists in S3 with async"""
        try:
            async with self.s3_session.client(
                's3',
                aws_access_key_id=self.config.aws_access_key_id,
                aws_secret_access_key=self.config.aws_secret_access_key,
                region_name=self.config.aws_region
            ) as s3_client:
                await s3_client.head_object(Bucket=self.config.s3_bucket, Key=s3_key)
                return True
        except Exception:
            return False

    async def upload_to_s3_async(self, file_path: str, s3_key: str) -> Optional[str]:
        """Upload file to S3 with async and retry logic"""
        for attempt in range(self.config.max_retries):
            try:
                async with self.s3_session.client(
                    's3',
                    aws_access_key_id=self.config.aws_access_key_id,
                    aws_secret_access_key=self.config.aws_secret_access_key,
                    region_name=self.config.aws_region
                ) as s3_client:
                    
                    # Upload with multipart for larger files
                    file_size = os.path.getsize(file_path)
                    
                    if file_size > 100 * 1024 * 1024:  # 100MB
                        # Use multipart upload for large files
                        await self._multipart_upload(s3_client, file_path, s3_key)
                    else:
                        # Regular upload for smaller files
                        with open(file_path, 'rb') as f:
                            await s3_client.upload_fileobj(
                                f, 
                                self.config.s3_bucket, 
                                s3_key,
                                ExtraArgs={'ContentType': 'audio/wav'}
                            )
                    
                    return f"s3://{self.config.s3_bucket}/{s3_key}"
                    
            except Exception as e:
                logger.warning(f"S3 upload attempt {attempt + 1} failed: {e}")
                if attempt == self.config.max_retries - 1:
                    logger.error(f"S3 upload failed after {self.config.max_retries} attempts")
                    return None
                await asyncio.sleep(2 ** attempt)  # Exponential backoff
        
        return None

    async def _multipart_upload(self, s3_client, file_path: str, s3_key: str):
        """Multipart upload for large files"""
        try:
            response = await s3_client.create_multipart_upload(
                Bucket=self.config.s3_bucket,
                Key=s3_key,
                ContentType='audio/wav'
            )
            upload_id = response['UploadId']
            
            parts = []
            part_number = 1
            
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(self.config.chunk_size * 1024)  # 8MB chunks
                    if not chunk:
                        break
                    
                    part_response = await s3_client.upload_part(
                        Bucket=self.config.s3_bucket,
                        Key=s3_key,
                        PartNumber=part_number,
                        UploadId=upload_id,
                        Body=chunk
                    )
                    
                    parts.append({
                        'ETag': part_response['ETag'],
                        'PartNumber': part_number
                    })
                    
                    part_number += 1
            
            await s3_client.complete_multipart_upload(
                Bucket=self.config.s3_bucket,
                Key=s3_key,
                UploadId=upload_id,
                MultipartUpload={'Parts': parts}
            )
            
        except Exception as e:
            # Cleanup failed multipart upload
            try:
                await s3_client.abort_multipart_upload(
                    Bucket=self.config.s3_bucket,
                    Key=s3_key,
                    UploadId=upload_id
                )
            except:
                pass
            raise e

    def get_video_info(self, video_url: str) -> Tuple[str, str, str]:
        """Extract video information with caching"""
        cache_key = hashlib.md5(video_url.encode()).hexdigest()
        cache_file = os.path.join(self.temp_dir, f"info_{cache_key}.json")
        
        # Check cache first
        if os.path.exists(cache_file):
            try:
                with open(cache_file, 'r', encoding='utf-8') as f:
                    info = json.load(f)
                    return info['title'], info['uploader'], info['id']
            except:
                pass
        
        # Extract info if not cached
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'extract_flat': False,
        }
        
        try:
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                info = ydl.extract_info(video_url, download=False)
                video_title = info.get('title', 'Unknown')
                channel_name = info.get('uploader', 'Unknown')
                video_id = info.get('id', 'unknown')
                
                # Cache the info
                cache_data = {
                    'title': video_title,
                    'uploader': channel_name,
                    'id': video_id
                }
                try:
                    with open(cache_file, 'w', encoding='utf-8') as f:
                        json.dump(cache_data, f)
                except:
                    pass
                
                return video_title, channel_name, video_id
                
        except Exception as e:
            logger.error(f"Failed to extract video info: {e}")
            return "Unknown", "Unknown", "unknown"

    async def download_video(self, video_url: str) -> Tuple[str, bool, Optional[str], Optional[str]]:
        """Download and process single video with optimizations"""
        if not SecurityUtils.validate_url(video_url):
            return video_url, False, "Invalid URL", None
        
        # Random delay to avoid rate limiting
        await asyncio.sleep(random.uniform(0.5, 2.0))
        
        try:
            # Get video info
            video_title, channel_name, video_id = self.get_video_info(video_url)
            
            # Sanitize filenames
            safe_title = SecurityUtils.sanitize_filename(video_title)
            safe_channel = SecurityUtils.sanitize_filename(channel_name)
            
            # Create S3 key
            s3_key = f"{self.config.s3_folder}/{safe_channel}/{safe_title}_{video_id}.wav"
            
            # Check if already exists in S3
            if await self.check_s3_file_exists(s3_key):
                logger.info(f"⏭ Already exists: {safe_title}")
                self.log_to_csv(safe_channel, video_url, "skipped", "exists_in_s3")
                self.stats['skipped'] += 1
                return video_url, True, "exists", None
            
            # Download and convert
            output_path = await self._download_and_convert(video_url, safe_title, video_id)
            
            if not output_path or not os.path.exists(output_path):
                self.log_to_csv(safe_channel, video_url, "error", "download_failed")
                self.stats['errors'] += 1
                return video_url, False, "Download failed", None
            
            # Upload to S3
            s3_url = await self.upload_to_s3_async(output_path, s3_key)
            
            # Cleanup local file
            try:
                os.remove(output_path)
            except:
                pass
            
            if s3_url:
                logger.info(f"✅ Completed: {safe_title}")
                self.log_to_csv(safe_channel, video_url, "success", s3_url)
                self.stats['success'] += 1
                return video_url, True, None, s3_url
            else:
                self.log_to_csv(safe_channel, video_url, "s3_error", "upload_failed")
                self.stats['errors'] += 1
                return video_url, False, "S3 upload failed", None
                
        except Exception as e:
            logger.error(f"Error processing {video_url}: {e}")
            self.log_to_csv("unknown", video_url, "error", str(e))
            self.stats['errors'] += 1
            return video_url, False, str(e), None

    async def _download_and_convert(self, video_url: str, safe_title: str, video_id: str) -> Optional[str]:
        """Download video and convert to WAV"""
        output_template = os.path.join(self.temp_dir, f"{safe_title}_{video_id}.%(ext)s")
        wav_file_path = os.path.join(self.temp_dir, f"{safe_title}_{video_id}.wav")
        
        ydl_opts = {
            'format': 'bestaudio[ext=m4a]/bestaudio[ext=mp3]/bestaudio/best',
            'outtmpl': output_template,
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'wav',
                'preferredquality': '16',  # Lower quality for faster processing
            }],
            'quiet': True,
            'no_warnings': True,
            'noplaylist': True,
            'extractaudio': True,
            'audioformat': 'wav',
            'prefer_ffmpeg': True,
        }
        
        try:
            # Run in thread pool to avoid blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self._download_with_ytdlp(ydl_opts, video_url)
            )
            
            return wav_file_path if os.path.exists(wav_file_path) else None
            
        except Exception as e:
            logger.error(f"Download/conversion error: {e}")
            return None

    def _download_with_ytdlp(self, ydl_opts: dict, video_url: str):
        """Download video using yt-dlp in thread"""
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])

    async def get_video_list_from_api(self) -> Tuple[List[str], Optional[str]]:
        """Get video list from API with retry logic"""
        for attempt in range(self.config.max_retries):
            try:
                async with self.session.get(f"{self.config.api_base_url}/get-video-list") as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    if data.get("status") == "success":
                        video_lines = data.get("video_list", [])
                        return video_lines, data.get("list_id")
                    else:
                        logger.warning(f"API returned error: {data}")
                        return [], None
                        
            except Exception as e:
                logger.warning(f"API request attempt {attempt + 1} failed: {e}")
                if attempt == self.config.max_retries - 1:
                    logger.error("Failed to get video list from API")
                    return [], None
                await asyncio.sleep(2 ** attempt)
        
        return [], None

    async def notify_api_completion(self, list_id: Optional[str], status: str, message: str = ""):
        """Notify API of completion status"""
        if not list_id:
            return
            
        try:
            payload = {
                "list_id": list_id,
                "status": status,
                "message": message,
                "timestamp": datetime.now().isoformat(),
                "stats": self.stats
            }
            
            async with self.session.post(
                f"{self.config.api_base_url}/notify-completion", 
                json=payload
            ) as response:
                response.raise_for_status()
                logger.info("API notified successfully")
                
        except Exception as e:
            logger.error(f"API notification error: {e}")

    def extract_urls(self, video_lines: List) -> List[str]:
        """Extract and validate URLs from various input formats"""
        video_urls = []
        
        for line in video_lines:
            video_url = ""
            
            if isinstance(line, dict):
                video_url = line.get('video_url', '')
            else:
                line = str(line).strip()
                if line.startswith(('https://', 'http://')):
                    video_url = line
                else:
                    parts = line.split('|')
                    video_url = parts[1].strip() if len(parts) >= 2 else ''
            
            if video_url and SecurityUtils.validate_url(video_url):
                video_urls.append(video_url)
            else:
                logger.warning(f"Invalid URL skipped: {video_url}")
        
        return video_urls

    def print_progress(self, completed: int, total: int, elapsed_time: float):
        """Print progress with statistics"""
        if total == 0:
            return
            
        progress = (completed / total) * 100
        bar_length = 40
        filled = int(bar_length * completed // total)
        bar = '█' * filled + '░' * (bar_length - filled)
        
        # Calculate ETA
        if completed > 0:
            avg_time = elapsed_time / completed
            remaining_time = avg_time * (total - completed)
            eta = f"{int(remaining_time//60)}m {int(remaining_time%60)}s"
        else:
            eta = "calculating..."
        
        print(f"\n[{bar}] {progress:.1f}% ({completed}/{total})")
        print(f"✅ Success: {self.stats['success']} | ⏭ Skipped: {self.stats['skipped']} | ❌ Errors: {self.stats['errors']}")
        print(f"⏰ Elapsed: {int(elapsed_time//60)}m {int(elapsed_time%60)}s | ETA: {eta}")
        print("-" * 60)

    async def process_videos(self):
        """Main processing function with optimizations"""
        logger.info("🔄 Getting video list from API...")
        video_lines, list_id = await self.get_video_list_from_api()
        
        if not video_lines:
            logger.error("❌ No video list received from API")
            return

        # Extract and validate URLs
        video_urls = self.extract_urls(video_lines)
        
        if not video_urls:
            logger.error("❌ No valid URLs found")
            return

        total_videos = len(video_urls)
        self.stats['total'] = total_videos
        
        logger.info(f"📊 Processing {total_videos} videos with {self.config.max_workers} workers")
        
        start_time = time.time()
        completed = 0
        
        # Process videos with controlled concurrency
        semaphore = asyncio.Semaphore(self.config.max_workers)
        
        async def process_with_semaphore(url):
            async with semaphore:
                return await self.download_video(url)
        
        # Create tasks
        tasks = [process_with_semaphore(url) for url in video_urls]
        
        # Process with progress tracking
        for coro in asyncio.as_completed(tasks):
            try:
                result = await coro
                completed += 1
                
                # Print progress every 10 completions or at the end
                if completed % 10 == 0 or completed == total_videos:
                    elapsed = time.time() - start_time
                    self.print_progress(completed, total_videos, elapsed)
                    
            except Exception as e:
                logger.error(f"Task failed: {e}")
                completed += 1
                self.stats['errors'] += 1

        # Final statistics
        total_time = time.time() - start_time
        self.print_final_stats(total_time)
        
        # Notify API
        status = "completed" if self.stats['errors'] == 0 else "partial"
        message = f"Processed: {self.stats['success']} new, {self.stats['skipped']} existing, {self.stats['errors']} errors"
        await self.notify_api_completion(list_id, status, message)

    def print_final_stats(self, total_time: float):
        """Print final statistics"""
        print(f"\n{'='*60}")
        print(f"🎉 PROCESSING COMPLETED!")
        print(f"📊 FINAL STATISTICS:")
        print(f"   • Total videos: {self.stats['total']}")
        print(f"   • ✅ Successful: {self.stats['success']}")
        print(f"   • ⏭ Already existing: {self.stats['skipped']}")
        print(f"   • ❌ Failed: {self.stats['errors']}")
        print(f"   • ⏰ Total time: {int(total_time//60)}m {int(total_time%60)}s")
        if self.stats['total'] > 0:
            print(f"   • ⚡ Average: {total_time/self.stats['total']:.1f}s/video")
        print(f"{'='*60}")

async def main():
    """Main async function"""
    config = Config()
    
    # Validate configuration
    if not config.validate():
        logger.error("❌ Invalid configuration. Please check environment variables.")
        return
    
    # Run downloader
    async with VideoDownloader(config) as downloader:
        await downloader.process_videos()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise
