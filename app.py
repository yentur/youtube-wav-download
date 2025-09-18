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

# S3 Configuration - Bu değerleri environment variable yapmanızı öneriyorum
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
        eta = d.get('_eta_str', 'N/A')
        print(f"  ⏳ {d['filename']} | {percent} | {speed} | ETA: {eta}", end="\r")
    elif d['status'] == 'finished':
        print(f"\n  ✅ Tamamlandı: {d['filename']}")

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

def download_single_video(video_url, video_title, channel_name, temp_dir):
    """Tek bir videoyu geçici klasöre indirip wav olarak kaydeder ve S3'e yükler."""
    time.sleep(random.uniform(1, 3))
    safe_title = "".join(c if c.isalnum() or c in " -_()" else "_" for c in video_title)
    
    # Geçici dosya yolları
    output_template = os.path.join(temp_dir, f"{safe_title}.%(ext)s")
    wav_file_path = os.path.join(temp_dir, f"{safe_title}.wav")

    # S3'te dosya var mı kontrol et
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=AWS_REGION
        )
        s3_key = f"{S3_FOLDER}/{channel_name}/{safe_title}.wav"
        
        if check_s3_file_exists(s3_client, S3_BUCKET, s3_key):
            print(f"  ⏭ Atlandı (S3'te zaten var): {safe_title}.wav")
            log_to_csv(channel_name, video_url, "skipped", "already_exists_in_s3")
            return (video_url, True, "Already exists in S3", f"s3://{S3_BUCKET}/{s3_key}")
    except Exception as e:
        print(f"⚠️ S3 kontrol hatası: {e}")

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
        print(f"🎵 İndiriliyor: {video_title}")
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([video_url])
        
        # WAV dosyasını S3'e yükle
        if os.path.exists(wav_file_path):
            s3_url = upload_wav_to_s3(wav_file_path, channel_name, f"{safe_title}.wav")
            
            # Geçici dosyayı sil
            try:
                os.remove(wav_file_path)
            except Exception as e:
                print(f"⚠️ Geçici dosya silinirken hata: {e}")
            
            if s3_url:
                log_to_csv(channel_name, video_url, "success", f"Uploaded to S3: {s3_url}")
                return (video_url, True, None, s3_url)
            else:
                log_to_csv(channel_name, video_url, "s3_error", "Failed to upload to S3")
                return (video_url, False, "S3 upload failed", None)
        else:
            log_to_csv(channel_name, video_url, "error", "WAV file not created")
            return (video_url, False, "WAV file not created", None)
            
    except Exception as e:
        log_to_csv(channel_name, video_url, "error", str(e))
        return (video_url, False, str(e), None)

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
    """API'den video listesi alır - retry mantığı ile"""
    def _get_video_list():
        print("📡 API'den video listesi alınıyor...")
        response = requests.get(f"{API_BASE_URL}/get-video-list", timeout=30)
        response.raise_for_status()
        
        data = response.json()
        if data.get("status") == "success":
            video_lines = data.get("video_list", [])
            print(f"✅ API'den {len(video_lines)} video alındı")
            return video_lines, data.get("list_id")
        else:
            print(f"❌ API'den hata: {data.get('message', 'Bilinmeyen hata')}")
            return [], None
    
    try:
        return api_request_with_retry(_get_video_list)
    except Exception as e:
        print(f"❌ API'den video listesi alınamadı: {e}")
        return [], None

def notify_api_completion(list_id, status, message=""):
    """API'ye işlem tamamlandığını bildirir - retry mantığı ile"""
    if not list_id:
        return
        
    def _notify_completion():
        payload = {
            "list_id": list_id,
            "status": status,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        response = requests.post(f"{API_BASE_URL}/notify-completion", json=payload, timeout=10)
        response.raise_for_status()
        print(f"✅ API'ye durum bildirildi: {status}")
        return True
    
    try:
        api_request_with_retry(_notify_completion)
    except Exception as e:
        print(f"⚠️ API'ye durum bildirme hatası: {e}")

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

def download_videos_from_api(max_workers=4):
    """API'den video listesi alarak videoları indirir ve doğrudan S3'e yükler"""
    video_lines, list_id = get_video_list_from_api()
    
    if not video_lines:
        print("❌ API'den video listesi alınamadı veya liste boş.")
        return

    videos_to_download = []

    # Geçici klasör oluştur
    temp_dir = tempfile.mkdtemp(prefix="youtube_downloads_")
    print(f"📁 Geçici klasör: {temp_dir}")

    for line in video_lines:
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
                print(f"📋 URL tespit edildi, video bilgisi çekiliyor: {line}")
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
                    continue

        if not all([channel_name, video_url, video_title]):
            print(f"⚠️ Eksik veri atlandı: {line}")
            continue

        print(f"📹 İşlenecek: [{channel_name}] {video_title}")
        videos_to_download.append((video_url, video_title, channel_name))

    print(f"Toplam {len(videos_to_download)} video işlenecek.")

    if not videos_to_download:
        notify_api_completion(list_id, "completed", "No videos to process")
        print("⚠️ İşlenecek video bulunamadı.")
        return

    success_count = 0
    error_count = 0
    skipped_count = 0
    uploaded_files = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_single_video, v_url, title, channel_name, temp_dir)
            for v_url, title, channel_name in videos_to_download
        ]
        
        for future in as_completed(futures):
            video_url, success, error, s3_url = future.result()
            if success and s3_url:
                if error == "Already exists in S3":
                    skipped_count += 1
                    print(f"  ⏭ Atlandı: {video_url}")
                else:
                    success_count += 1
                    uploaded_files.append(s3_url)
                    print(f"  ✅ İndirildi ve S3'e yüklendi: {video_url}")
            else:
                print(f"  ❌ Hata: {video_url} ({error})")
                error_count += 1

    # Geçici klasörü temizle
    try:
        import shutil
        shutil.rmtree(temp_dir)
        print(f"🗑️ Geçici klasör silindi: {temp_dir}")
    except Exception as e:
        print(f"⚠️ Geçici klasör silinirken hata: {e}")

    print(f"\n🎉 İşlem tamamlandı.")
    print(f"✅ Başarılı: {success_count}")
    print(f"⏭ Atlandı: {skipped_count}")
    print(f"❌ Hata: {error_count}")
    print(f"📑 Log dosyası: {os.path.abspath(LOG_FILE)}")
    print(f"☁️ S3'e yüklenen yeni dosya sayısı: {len(uploaded_files)}")

    # API'ye başarı durumunu bildir
    message = f"Successfully processed {success_count} new videos. Skipped {skipped_count} existing files. Uploaded {len(uploaded_files)} files to S3."
    if error_count > 0:
        message += f" {error_count} errors occurred."
    
    notify_api_completion(list_id, "completed" if error_count == 0 else "partial_success", message)

if __name__ == "__main__":
    download_videos_from_api(max_workers=8)
