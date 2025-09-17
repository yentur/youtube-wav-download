import yt_dlp
import os
import csv
import random, time
import boto3
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import threading
from typing import List, Dict, Tuple, Optional

LOG_FILE = "download_log.csv"

# S3 Configuration - Bu değerleri environment variable yapmanızı öneriyorum
S3_BUCKET = os.getenv("S3_BUCKET")
S3_FOLDER = os.getenv("S3_FOLDER")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")

# API Configuration
API_BASE_URL = os.getenv("API_BASE_URL", "http://localhost:8000")

# Global lock for thread-safe logging
log_lock = threading.Lock()

class DownloadStats:
    """İndirme istatistiklerini takip eder"""
    def __init__(self):
        self.total_videos = 0
        self.success_count = 0
        self.error_count = 0
        self.skipped_count = 0
        self.uploaded_files = []
        self.errors = []
        self._lock = threading.Lock()
    
    def add_success(self, s3_url: str):
        with self._lock:
            self.success_count += 1
            self.uploaded_files.append(s3_url)
    
    def add_error(self, video_url: str, error_msg: str):
        with self._lock:
            self.error_count += 1
            self.errors.append(f"{video_url}: {error_msg}")
    
    def add_skipped(self):
        with self._lock:
            self.skipped_count += 1
    
    def get_summary(self) -> Dict:
        with self._lock:
            return {
                "total_videos": self.total_videos,
                "success_count": self.success_count,
                "error_count": self.error_count,
                "skipped_count": self.skipped_count,
                "uploaded_files": len(self.uploaded_files),
                "errors": self.errors.copy()
            }

def progress_hook(d):
    """yt-dlp indirme ilerleme callback"""
    if d['status'] == 'downloading':
        percent = d.get('_percent_str', '').strip()
        speed = d.get('_speed_str', 'N/A')
        eta = d.get('_eta_str', 'N/A')
        print(f"  ⏳ {d['filename']} | {percent} | {speed} | ETA: {eta}", end="\r")
    elif d['status'] == 'finished':
        print(f"\n  ✅ Tamamlandı: {d['filename']}")

def log_to_csv(user, video_url, status, message=""):
    """Log dosyasına thread-safe yazar"""
    with log_lock:
        file_exists = os.path.isfile(LOG_FILE)
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(["timestamp", "user", "video_url", "status", "message"])
            writer.writerow([datetime.now().isoformat(), user, video_url, status, message])

def video_already_downloaded(video_title, channel_path):
    """Yerel klasörde wav dosyası var mı kontrol et."""
    safe_title = "".join(c if c.isalnum() or c in " -_()" else "_" for c in video_title)
    expected_file = os.path.join(channel_path, f"{safe_title}.wav")
    return os.path.exists(expected_file)

def ensure_s3_folder_exists(s3_client, bucket, folder_key):
    """S3'te klasör var mı kontrol et, yoksa oluştur"""
    try:
        s3_client.head_object(Bucket=bucket, Key=folder_key + "/")
        print(f"📁 S3 klasörü mevcut: {folder_key}")
    except:
        s3_client.put_object(Bucket=bucket, Key=folder_key + "/")
        print(f"📁 S3 klasörü oluşturuldu: {folder_key}")

def upload_wav_to_s3(file_path, channel_name, filename):
    """WAV dosyasını S3'e yükler"""
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )

        # S3'te klasör yapısı: S3_FOLDER/channel_name/filename.wav
        s3_key = f"{S3_FOLDER}/{channel_name}/{filename}"
        
        # Klasörün var olduğundan emin ol
        ensure_s3_folder_exists(s3_client, S3_BUCKET, f"{S3_FOLDER}/{channel_name}")

        print(f"☁️ S3'e yükleniyor: {file_path} -> s3://{S3_BUCKET}/{s3_key}")

        file_size = os.path.getsize(file_path)
        with open(file_path, 'rb') as f:
            s3_client.upload_fileobj(f, S3_BUCKET, s3_key)

        print(f"✅ S3 yükleme tamamlandı: s3://{S3_BUCKET}/{s3_key} ({file_size} bytes)")
        return f"s3://{S3_BUCKET}/{s3_key}"
        
    except Exception as e:
        print(f"❌ S3 yükleme hatası: {file_path} - {e}")
        return None

def download_single_video(video_url, video_title, channel_path, user, channel_name, stats: DownloadStats, cookie_file=None):
    """Tek bir videoyu indirip wav olarak kaydeder ve S3'e yükler."""
    time.sleep(random.uniform(2, 5))
    safe_title = "".join(c if c.isalnum() or c in " -_()" else "_" for c in video_title)
    output_template = os.path.join(channel_path, f"{safe_title}.%(ext)s")
    wav_file_path = os.path.join(channel_path, f"{safe_title}.wav")

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

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        
        # WAV dosyasını S3'e yükle
        s3_url = upload_wav_to_s3(wav_file_path, channel_name, f"{safe_title}.wav")
        
        if s3_url:
            # Başarılı yükleme sonrası yerel dosyayı sil
            try:
                os.remove(wav_file_path)
                print(f"🗑️ Yerel dosya silindi: {wav_file_path}")
            except Exception as e:
                print(f"⚠️ Yerel dosya silinirken hata: {e}")
            
            log_to_csv(user, video_url, "success", f"Uploaded to S3: {s3_url}")
            stats.add_success(s3_url)
            return (video_url, True, None, s3_url)
        else:
            error_msg = "S3 upload failed"
            log_to_csv(user, video_url, "s3_error", error_msg)
            stats.add_error(video_url, error_msg)
            return (video_url, False, error_msg, None)
            
    except Exception as e:
        error_msg = str(e)
        log_to_csv(user, video_url, "error", error_msg)
        stats.add_error(video_url, error_msg)
        return (video_url, False, error_msg, None)

def api_request_with_retry(func, max_retries=3, delay=5):
    """API isteklerini retry mantığı ile yapar"""
    for attempt in range(max_retries):
        try:
            return func()
        except requests.exceptions.RequestException as e:
            print(f"❌ API bağlantı hatası (deneme {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                print(f"⏳ {delay} saniye bekleniyor...")
                time.sleep(delay)
            else:
                print("❌ Tüm denemeler başarısız oldu")
                raise e

def get_video_list_from_api():
    """API'den video listesi alır - güncellenmiş versiyon"""
    def _get_video_list():
        print("📡 API'den video listesi alınıyor...")
        response = requests.get(f"{API_BASE_URL}/get-video-list", timeout=30)
        response.raise_for_status()
        
        data = response.json()
        print(f"📄 API Response: {data.get('status', 'unknown')}")
        
        if data.get("status") == "success":
            video_lines = data.get("video_list", [])
            list_id = data.get("list_id")
            filename = data.get("filename", "unknown")
            video_count = data.get("video_count", len(video_lines))
            
            print(f"✅ API'den alınan liste:")
            print(f"   📋 Dosya: {filename}")
            print(f"   🆔 List ID: {list_id}")
            print(f"   📹 Video Sayısı: {video_count}")
            print(f"   🕐 Başlangıç: {data.get('start_time', 'N/A')}")
            
            return video_lines, list_id, {
                "filename": filename,
                "video_count": video_count,
                "start_time": data.get("start_time")
            }
        elif data.get("status") == "no_more_files":
            print(f"ℹ️ {data.get('message', 'Tüm dosyalar işlendi')}")
            return [], None, {"message": data.get("message")}
        else:
            print(f"❌ API'den beklenmeyen yanıt: {data}")
            return [], None, {"error": data.get('message', 'Bilinmeyen hata')}
    
    try:
        return api_request_with_retry(_get_video_list)
    except Exception as e:
        print(f"❌ API'den video listesi alınamadı: {e}")
        return [], None, {"error": str(e)}

def notify_api_completion(list_id: str, status: str, video_count: int, stats: DownloadStats, message: str = ""):
    """API'ye işlem tamamlandığını bildirir - güncellenmiş versiyon"""
    if not list_id:
        print("⚠️ List ID bulunamadı, API bildirimi yapılamıyor")
        return False
        
    def _notify_completion():
        # İstatistikleri al
        summary = stats.get_summary()
        
        # Detaylı mesaj hazırla
        if not message:
            if status == "completed":
                message = f"Successfully processed {summary['success_count']}/{video_count} videos"
                if summary['skipped_count'] > 0:
                    message += f" ({summary['skipped_count']} skipped)"
                if summary['uploaded_files'] > 0:
                    message += f". Uploaded {summary['uploaded_files']} files to S3"
            elif status == "error":
                message = f"Failed to process videos. {summary['error_count']} errors occurred"
            elif status == "partial_success":
                message = f"Partially completed: {summary['success_count']} success, {summary['error_count']} errors"
        
        # Hata detaylarını hazırla
        error_details = None
        if summary['errors']:
            # İlk 5 hatayı al (çok uzun olmasın diye)
            error_details = "; ".join(summary['errors'][:5])
            if len(summary['errors']) > 5:
                error_details += f"; ... and {len(summary['errors']) - 5} more errors"
        
        payload = {
            "list_id": list_id,
            "status": status,
            "message": message,
            "timestamp": datetime.now().isoformat(),
            "video_count": summary['success_count'] + summary['error_count'],  # İşlenen video sayısı
            "error_details": error_details
        }
        
        print(f"📤 API'ye bildirim gönderiliyor:")
        print(f"   🆔 List ID: {list_id}")
        print(f"   📊 Status: {status}")
        print(f"   📹 Processed: {payload['video_count']} videos")
        print(f"   ✅ Success: {summary['success_count']}")
        print(f"   ❌ Errors: {summary['error_count']}")
        print(f"   ⏭ Skipped: {summary['skipped_count']}")
        
        response = requests.post(f"{API_BASE_URL}/notify-completion", json=payload, timeout=30)
        response.raise_for_status()
        
        # API'nin yanıtını kontrol et
        response_data = response.json()
        if response_data.get("status") == "success":
            print(f"✅ API bildirimi başarılı!")
            
            # Tamamlanan işlem bilgilerini göster
            completed = response_data.get("completed_process", {})
            if completed:
                print(f"   📁 Dosya: {completed.get('filename', 'N/A')}")
                print(f"   ⏱ Süre: {completed.get('duration_seconds', 'N/A')} saniye")
            
            # Sistem durumu bilgilerini göster
            current_state = response_data.get("current_state", {})
            if current_state:
                print(f"   🔄 Aktif İşlemler: {current_state.get('active_processes', 0)}")
                print(f"   ✅ İşlenen Dosyalar: {current_state.get('processed_files', 0)}")
                print(f"   📋 Kalan Dosyalar: {current_state.get('remaining_files', 0)}")
        else:
            print(f"⚠️ API bildiriminde uyarı: {response_data.get('message', 'Unknown')}")
        
        return True
    
    try:
        return api_request_with_retry(_notify_completion)
    except Exception as e:
        print(f"❌ API'ye durum bildirme hatası: {e}")
        return False

def get_video_info(video_url):
    """Video URL'sinden video bilgilerini çeker"""
    try:
        ydl_opts = {
            'quiet': True,
            'no_warnings': True,
        }
        
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(video_url, download=False)
            
            video_title = info.get('title', 'Unknown Title')
            channel_name = info.get('uploader', 'Unknown Channel')
            
            # Güvenli dosya adı oluştur
            safe_channel = "".join(c if c.isalnum() or c in " -_()" else "_" for c in channel_name)
            
            return safe_channel, video_title
    except Exception as e:
        print(f"⚠️ Video bilgisi alınamadı: {video_url} - {e}")
        return "Unknown_Channel", "Unknown_Title"

def download_videos_from_api(download_dir='downloads', max_workers=1):
    """API'den video listesi alarak videoları indirir ve doğrudan S3'e yükler - güncellenmiş versiyon"""
    print(f"🚀 Video indirme işlemi başlatılıyor...")
    print(f"📁 İndirme klasörü: {download_dir}")
    print(f"🔧 Max worker: {max_workers}")
    print(f"🌐 API URL: {API_BASE_URL}")
    
    # API'den video listesi al
    video_lines, list_id, metadata = get_video_list_from_api()
    
    if not video_lines:
        if metadata.get("message"):
            print(f"ℹ️ {metadata['message']}")
        else:
            print("❌ API'den video listesi alınamadı veya liste boş.")
        return

    # İstatistik takibi başlat
    stats = DownloadStats()
    stats.total_videos = len(video_lines)
    
    print(f"\n📋 İşlenecek video listesi:")
    print(f"   📁 Dosya: {metadata.get('filename', 'N/A')}")
    print(f"   🆔 List ID: {list_id}")
    print(f"   📹 Toplam Video: {stats.total_videos}")

    videos_to_download = []

    # Video listesini işle
    for i, line in enumerate(video_lines, 1):
        print(f"\n📹 Video {i}/{len(video_lines)} işleniyor...")
        
        if isinstance(line, dict):
            # JSON formatında geliyorsa
            channel_name = line.get('channel_name', '')
            video_url = line.get('video_url', '')
            video_title = line.get('video_title', '')
        else:
            # String formatında geliyorsa
            line = line.strip()
            
            # URL olarak kontrol et
            if line.startswith('https://') or line.startswith('http://'):
                print(f"🔍 URL tespit edildi, video bilgisi çekiliyor: {line}")
                channel_name, video_title = get_video_info(line)
                video_url = line
            else:
                # Pipe separated format (channel_name|video_url|video_title)
                parts = line.split('|')
                if len(parts) >= 3:
                    channel_name = parts[0].strip()
                    video_url = parts[1].strip()
                    video_title = parts[2].strip()
                else:
                    print(f"⚠️ Geçersiz format atlandı: {line}")
                    stats.add_error(line, "Invalid format")
                    continue

        if not all([channel_name, video_url, video_title]):
            print(f"⚠️ Eksik veri atlandı: {line}")
            stats.add_error(str(line), "Missing data")
            continue

        print(f"   📺 Kanal: {channel_name}")
        print(f"   🎬 Başlık: {video_title}")
        print(f"   🔗 URL: {video_url}")

        channel_path = os.path.join(download_dir, channel_name)
        os.makedirs(channel_path, exist_ok=True)

        if video_already_downloaded(video_title, channel_path):
            print(f"   ⏭ Atlandı (zaten var): {video_title}.wav")
            log_to_csv(channel_name, video_url, "skipped", "already_downloaded")
            stats.add_skipped()
            continue

        videos_to_download.append((video_url, video_title, channel_path, channel_name))

    print(f"\n📊 İndirme Özeti:")
    print(f"   📋 Toplam Video: {stats.total_videos}")
    print(f"   📥 İndirilecek: {len(videos_to_download)}")
    print(f"   ⏭ Atlandı: {stats.skipped_count}")

    if not videos_to_download:
        # Hiç indirme olmasa bile başarılı sayalım (çünkü işlem tamamlandı)
        success = notify_api_completion(list_id, "completed", stats.total_videos, stats, 
                                       "No new videos to download - all already exist")
        print("✅ Tüm videolar zaten mevcut, işlem tamamlandı.")
        return

    # Paralel indirme başlat
    print(f"\n🔄 {len(videos_to_download)} video indiriliyor (max {max_workers} thread)...")
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_single_video, v_url, title, channel_path, channel_name, channel_name, stats, None)
            for v_url, title, channel_path, channel_name in videos_to_download
        ]
        
        completed_count = 0
        for future in as_completed(futures):
            completed_count += 1
            video_url, success, error, s3_url = future.result()
            
            print(f"\n📊 İlerleme: {completed_count}/{len(videos_to_download)}")
            if success and s3_url:
                print(f"   ✅ Başarılı: {video_url}")
            else:
                print(f"   ❌ Hata: {video_url} ({error})")

    # Final istatistikleri
    final_stats = stats.get_summary()
    print(f"\n🎉 İndirme İşlemi Tamamlandı!")
    print(f"   ✅ Başarılı: {final_stats['success_count']}")
    print(f"   ❌ Hata: {final_stats['error_count']}")
    print(f"   ⏭ Atlandı: {final_stats['skipped_count']}")
    print(f"   ☁️ S3'e Yüklenen: {final_stats['uploaded_files']}")
    print(f"   📑 Log Dosyası: {os.path.abspath(LOG_FILE)}")

    # Hata detaylarını göster
    if final_stats['errors']:
        print(f"\n❌ Hata Detayları:")
        for i, error in enumerate(final_stats['errors'][:5], 1):  # İlk 5 hatayı göster
            print(f"   {i}. {error}")
        if len(final_stats['errors']) > 5:
            print(f"   ... ve {len(final_stats['errors']) - 5} hata daha")

    # Boş klasörleri temizle
    try:
        cleaned_dirs = 0
        for root, dirs, files in os.walk(download_dir, topdown=False):
            if not files and not dirs and root != download_dir:
                os.rmdir(root)
                cleaned_dirs += 1
        if cleaned_dirs > 0:
            print(f"🗑️ {cleaned_dirs} boş klasör temizlendi")
    except Exception as e:
        print(f"⚠️ Klasör temizleme hatası: {e}")

    # API'ye durum bildir
    if final_stats['error_count'] == 0:
        status = "completed"
    elif final_stats['success_count'] > 0:
        status = "partial_success"
    else:
        status = "error"
    
    success = notify_api_completion(list_id, status, stats.total_videos, stats)
    
    if success:
        print(f"✅ İşlem durumu API'ye başarıyla bildirildi")
    else:
        print(f"⚠️ API bildirimi başarısız oldu")

def continuous_download_mode(download_dir='downloads', max_workers=1, poll_interval=30):
    """Sürekli API'yi kontrol ederek yeni video listelerini işler"""
    print(f"🔄 Sürekli indirme modu başlatılıyor...")
    print(f"📡 Polling aralığı: {poll_interval} saniye")
    print(f"⏹ Durdurmak için Ctrl+C")
    
    try:
        while True:
            try:
                download_videos_from_api(download_dir, max_workers)
                print(f"\n⏳ {poll_interval} saniye bekleniyor...")
                time.sleep(poll_interval)
            except KeyboardInterrupt:
                print("\n🛑 Kullanıcı tarafından durduruldu")
                break
            except Exception as e:
                print(f"\n❌ Beklenmeyen hata: {e}")
                print(f"⏳ 60 saniye bekleniyor...")
                time.sleep(60)
    except KeyboardInterrupt:
        print("\n👋 Sürekli indirme modu sonlandırıldı")

if __name__ == "__main__":
    # Çoklu işlem desteği test modu
    # Sürekli mod için: continuous_download_mode(max_workers=4, poll_interval=30)
    download_videos_from_api(max_workers=8)
