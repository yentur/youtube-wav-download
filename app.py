import yt_dlp
import os
import csv
import random, time
import boto3
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import tempfile

LOG_FILE = "download_log.csv"

# S3 Configuration
S3_BUCKET = os.getenv("S3_BUCKET")
S3_FOLDER = os.getenv("S3_FOLDER")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

# API Configuration
API_BASE_URL = os.getenv("API_BASE_URL")

def progress_hook(d):
    """yt-dlp indirme ilerleme callback"""
    if d['status'] == 'downloading':
        percent = d.get('_percent_str', '').strip()
        speed = d.get('_speed_str', 'N/A')
        print(f"⏳ {percent} | {speed}", end="\r")
    elif d['status'] == 'finished':
        print(f"✅ İndirildi")

def log_to_csv(user, video_url, status, message=""):
    """Log dosyasına yazar"""
    file_exists = os.path.isfile(LOG_FILE)
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "user", "video_url", "status", "message"])
        writer.writerow([datetime.now().isoformat(), user, video_url, status, message])

def check_s3_file_exists(s3_client, bucket, key):
    """S3'te dosya var mı kontrol et"""
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except:
        return False

def upload_wav_to_s3(file_path, s3_key):
    """WAV dosyasını S3'e yükler"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        with open(file_path, 'rb') as f:
            s3_client.upload_fileobj(f, S3_BUCKET, s3_key)

        return f"s3://{S3_BUCKET}/{s3_key}"
        
    except Exception as e:
        print(f"❌ S3 yükleme hatası: {e}")
        return None

def download_and_upload_video(video_url, temp_dir):
    """Video indir ve S3'e yükle"""
    time.sleep(random.uniform(1, 3))
    
    try:
        # Video bilgisini al
        ydl_opts_info = {'quiet': True, 'no_warnings': True}
        with yt_dlp.YoutubeDL(ydl_opts_info) as ydl:
            info = ydl.extract_info(video_url, download=False)
            video_title = info.get('title', 'Unknown')
            channel_name = info.get('uploader', 'Unknown')
        
        # Güvenli dosya adları
        safe_title = "".join(c if c.isalnum() or c in " -_()" else "_" for c in video_title)
        safe_channel = "".join(c if c.isalnum() or c in " -_()" else "_" for c in channel_name)
        
        # S3 yolu
        s3_key = f"{S3_FOLDER}/{safe_channel}/{safe_title}.wav"
        
        # S3'te var mı kontrol et
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        
        if check_s3_file_exists(s3_client, S3_BUCKET, s3_key):
            print(f"⏭ Zaten var: {safe_title}")
            log_to_csv(safe_channel, video_url, "skipped", "exists_in_s3")
            return (video_url, True, "exists", None)
        
        # Geçici dosya yolları
        output_template = os.path.join(temp_dir, f"{safe_title}.%(ext)s")
        wav_file_path = os.path.join(temp_dir, f"{safe_title}.wav")
        
        # İndir ve WAV'a çevir
        ydl_opts = {
            'format': 'bestaudio/best',
            'outtmpl': output_template,
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'wav',
                'preferredquality': '192',
            }],
            'quiet': True,
            'noplaylist': True,
            'progress_hooks': [progress_hook],
        }
        
        print(f"🎵 {safe_title}")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        
        # S3'e yükle
        if os.path.exists(wav_file_path):
            s3_url = upload_wav_to_s3(wav_file_path, s3_key)
            
            # Geçici dosyayı sil
            os.remove(wav_file_path)
            
            if s3_url:
                print(f"☁️ S3'e yüklendi")
                log_to_csv(safe_channel, video_url, "success", s3_url)
                return (video_url, True, None, s3_url)
            else:
                log_to_csv(safe_channel, video_url, "s3_error", "upload_failed")
                return (video_url, False, "S3 upload failed", None)
        else:
            log_to_csv(safe_channel, video_url, "error", "wav_not_created")
            return (video_url, False, "WAV not created", None)
            
    except Exception as e:
        print(f"❌ Hata: {str(e)}")
        log_to_csv("unknown", video_url, "error", str(e))
        return (video_url, False, str(e), None)

def get_video_list_from_api():
    """API'den video listesi al"""
    try:
        response = requests.get(f"{API_BASE_URL}/get-video-list", timeout=30)
        response.raise_for_status()
        
        data = response.json()
        if data.get("status") == "success":
            video_lines = data.get("video_list", [])
            return video_lines, data.get("list_id")
        else:
            return [], None
    except Exception as e:
        print(f"❌ API hatası: {e}")
        return [], None

def notify_api_completion(list_id, status, message=""):
    """API'ye durum bildir"""
    if not list_id:
        return
        
    try:
        payload = {
            "list_id": list_id,
            "status": status,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        response = requests.post(f"{API_BASE_URL}/notify-completion", json=payload, timeout=10)
        response.raise_for_status()
    except Exception as e:
        print(f"⚠️ API bildirim hatası: {e}")

def download_videos_from_api(max_workers=4):
    """Ana fonksiyon"""
    video_lines, list_id = get_video_list_from_api()
    
    if not video_lines:
        print("❌ Video listesi alınamadı")
        return

    # URL'leri çıkar
    video_urls = []
    for line in video_lines:
        if isinstance(line, dict):
            video_url = line.get('video_url', '')
        else:
            line = line.strip()
            if line.startswith('https://') or line.startswith('http://'):
                video_url = line
            else:
                parts = line.split('|')
                video_url = parts[1].strip() if len(parts) >= 2 else ''
        
        if video_url:
            video_urls.append(video_url)

    if not video_urls:
        print("❌ Geçerli URL bulunamadı")
        return

    print(f"📊 {len(video_urls)} video işlenecek")

    # Geçici klasör
    temp_dir = tempfile.mkdtemp(prefix="yt_")

    success_count = 0
    error_count = 0
    skipped_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_and_upload_video, url, temp_dir)
            for url in video_urls
        ]
        
        for future in as_completed(futures):
            video_url, success, error, s3_url = future.result()
            if success:
                if error == "exists":
                    skipped_count += 1
                else:
                    success_count += 1
            else:
                error_count += 1

    # Temizlik
    try:
        import shutil
        shutil.rmtree(temp_dir)
    except:
        pass

    print(f"\n🎉 Tamamlandı: ✅{success_count} ⏭{skipped_count} ❌{error_count}")

    # API'ye bildir
    message = f"Processed: {success_count} new, {skipped_count} existing, {error_count} errors"
    notify_api_completion(list_id, "completed" if error_count == 0 else "partial", message)

if __name__ == "__main__":
    download_videos_from_api(max_workers=8)
