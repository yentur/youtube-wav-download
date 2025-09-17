import yt_dlp
import os
import csv
import random, time
import hashlib
import tarfile
import boto3
import requests
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

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

def video_already_downloaded(video_title, channel_path):
    """Yerel klasörde wav dosyası var mı kontrol et."""
    safe_title = "".join(c if c.isalnum() or c in " -_()" else "_" for c in video_title)
    expected_file = os.path.join(channel_path, f"{safe_title}.wav")
    return os.path.exists(expected_file)

def download_single_video(video_url, video_title, channel_path, user, cookie_file=None):
    """Tek bir videoyu indirip wav olarak kaydeder."""
    time.sleep(random.uniform(2, 5))
    safe_title = "".join(c if c.isalnum() or c in " -_()" else "_" for c in video_title)
    output_template = os.path.join(channel_path, f"{safe_title}.%(ext)s")

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
        log_to_csv(user, video_url, "success")
        return (video_url, True, None)
    except Exception as e:
        log_to_csv(user, video_url, "error", str(e))
        return (video_url, False, str(e))

def create_archive_hash(directory_path):
    """Klasör içeriğinden hash oluşturur"""
    hasher = hashlib.md5()
    for root, dirs, files in os.walk(directory_path):
        for file in sorted(files):
            file_path = os.path.join(root, file)
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hasher.update(chunk)
    return hasher.hexdigest()

def create_tar_gz(source_dir, output_path):
    """Klasörü tar.gz olarak sıkıştırır"""
    print(f"🗜️ Sıkıştırılıyor: {source_dir} -> {output_path}")
    with tarfile.open(output_path, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))
    print(f"✅ Sıkıştırma tamamlandı: {output_path}")

def ensure_s3_folder_exists(s3_client, bucket, folder_key):
    """S3'te klasör var mı kontrol et, yoksa oluştur"""
    try:
        s3_client.head_object(Bucket=bucket, Key=folder_key + "/")
        print(f"📁 S3 klasörü mevcut: {folder_key}")
    except:
        s3_client.put_object(Bucket=bucket, Key=folder_key + "/")
        print(f"📁 S3 klasörü oluşturuldu: {folder_key}")

def upload_to_s3(file_path, s3_key):
    """Dosyayı S3'e yükler"""
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    ensure_s3_folder_exists(s3_client, S3_BUCKET, S3_FOLDER)

    full_s3_key = f"{S3_FOLDER}/{s3_key}"

    print(f"☁️ S3'e yükleniyor: {file_path} -> s3://{S3_BUCKET}/{full_s3_key}")

    file_size = os.path.getsize(file_path)
    with open(file_path, 'rb') as f:
        s3_client.upload_fileobj(f, S3_BUCKET, full_s3_key)

    print(f"✅ S3 yükleme tamamlandı: s3://{S3_BUCKET}/{full_s3_key} ({file_size} bytes)")
    return f"s3://{S3_BUCKET}/{full_s3_key}"

def get_video_list_from_api():
    """API'den video listesi alır"""
    try:
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
            
    except requests.exceptions.RequestException as e:
        print(f"❌ API bağlantı hatası: {e}")
        return [], None
    except json.JSONDecodeError as e:
        print(f"❌ API response parse hatası: {e}")
        return [], None

def notify_api_completion(list_id, status, message=""):
    """API'ye işlem tamamlandığını bildirir"""
    try:
        payload = {
            "list_id": list_id,
            "status": status,
            "message": message,
            "timestamp": datetime.now().isoformat()
        }
        response = requests.post(f"{API_BASE_URL}/notify-completion", json=payload, timeout=10)
        response.raise_for_status()
        print(f"✅ API'ye durum bildirildi: {status}")
    except Exception as e:
        print(f"⚠️ API'ye durum bildirme hatası: {e}")

def download_videos_from_api(download_dir='downloads', max_workers=1):
    """API'den video listesi alarak videoları indirir"""
    video_lines, list_id = get_video_list_from_api()
    
    if not video_lines:
        print("❌ API'den video listesi alınamadı veya liste boş.")
        return

    videos_to_download = []

    for line in video_lines:
        if isinstance(line, dict):
            # JSON formatında geliyorsa
            channel_name = line.get('channel_name', '')
            video_url = line.get('video_url', '')
            video_title = line.get('video_title', '')
        else:
            # String formatında geliyorsa (channel_name|video_url|video_title)
            parts = line.split('|')
            if len(parts) >= 3:
                channel_name = parts[0]
                video_url = parts[1]
                video_title = parts[2]
            else:
                print(f"⚠️ Geçersiz format atlandı: {line}")
                continue

        if not all([channel_name, video_url, video_title]):
            print(f"⚠️ Eksik veri atlandı: {line}")
            continue

        channel_path = os.path.join(download_dir, channel_name)
        os.makedirs(channel_path, exist_ok=True)

        if video_already_downloaded(video_title, channel_path):
            print(f"  ⏭ Atlandı (zaten var): {video_title}.wav")
            log_to_csv(channel_name, video_url, "skipped", "already_downloaded")
            continue

        videos_to_download.append((video_url, video_title, channel_path, channel_name))

    print(f"Toplam {len(videos_to_download)} video indirilecek.")

    if not videos_to_download:
        notify_api_completion(list_id, "completed", "No new videos to download")
        print("⚠️ İndirilecek yeni video bulunamadı.")
        return

    success_count = 0
    error_count = 0

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(download_single_video, v_url, title, channel_path, channel_name, None)
            for v_url, title, channel_path, channel_name in videos_to_download
        ]
        for future in as_completed(futures):
            video_url, success, error = future.result()
            if success:
                print(f"  ✅ İndirildi: {video_url}")
                success_count += 1
            else:
                print(f"  ❌ Hata: {video_url} ({error})")
                error_count += 1

    print(f"\n🎉 İşlem tamamlandı. Başarılı: {success_count}, Hata: {error_count}")
    print(f"📑 Log dosyası: {os.path.abspath(LOG_FILE)}")

    # S3'e yükleme işlemi
    if os.path.exists(download_dir) and os.listdir(download_dir):
        try:
            # Hash oluştur
            archive_hash = create_archive_hash(download_dir)

            # Tar.gz dosyası oluştur
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            archive_name = f"youtube_dataset_{timestamp}_{archive_hash[:8]}.tar.gz"
            archive_path = os.path.join(os.getcwd(), archive_name)

            create_tar_gz(download_dir, archive_path)

            # S3'e yükle
            s3_url = upload_to_s3(archive_path, archive_name)
            print(f"🌐 S3 URL: {s3_url}")

            # Yerel tar.gz dosyasını sil
            os.remove(archive_path)
            print(f"🗑️ Yerel arşiv dosyası silindi: {archive_path}")

            # API'ye başarı durumunu bildir
            notify_api_completion(list_id, "completed", f"Successfully processed {success_count} videos. S3 URL: {s3_url}")

        except Exception as e:
            print(f"❌ S3 yükleme hatası: {e}")
            notify_api_completion(list_id, "error", f"S3 upload failed: {str(e)}")
    else:
        notify_api_completion(list_id, "completed", "No files to upload")
        print("⚠️ İndirilmiş dosya bulunamadı, S3 yüklemesi yapılmadı.")

if __name__ == "__main__":
    download_videos_from_api(max_workers=8)
