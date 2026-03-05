import os
import requests
import json
import logging
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = 'kafka:9092' 
TOPIC = 'raw_hcm_camera'

# Thư mục lưu ảnh tĩnh (Sẽ map vào Data Lake khi dùng Docker)
IMAGE_SAVE_DIR = "/opt/airflow/data/raw_cameras"

# Danh sách Camera với URL đầy đủ
CAMERAS = {
    "Nga_Tu_Hang_Xanh": "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx?id=5d9ddd49766c880017188c94&bg=black&w=300&h=230",
    "Vong_Xoay_Lang_Cha_Ca": "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx?id=5d8cdbdc766c88001718896a&bg=black&w=300&h=230&t=1772643354878",
    "Cau_Kenh_Te": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=63ae7669bfd3d90017e8f0d9&t=1772643466929",
    "Nga_Tu_Thu_Duc": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=56df81d8c062921100c143de&t=1772643506191"
}

def setup_directory():
    """Tạo thư mục lưu ảnh nếu chưa có"""
    if not os.path.exists(IMAGE_SAVE_DIR):
        os.makedirs(IMAGE_SAVE_DIR)

def main():
    setup_directory()
    
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=3
        )
        
        # --- CƠ CHẾ VƯỢT TƯỜNG LỬA CHỐNG BOT ---
        session = requests.Session()
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            "Referer": "https://giaothong.hochiminhcity.gov.vn/map.aspx",
            "Connection": "keep-alive"
        }
        session.headers.update(headers)
        
        # Gọi trang chủ để lấy Cookie (Rất quan trọng để không bị chặn)
        try:
            session.get("https://giaothong.hochiminhcity.gov.vn/", timeout=10)
        except Exception as e:
            logging.warning(f"[Camera] Lỗi khi lấy cookie ban đầu: {e}")
        # ----------------------------------------

        timestamp_now = datetime.utcnow()
        timestamp_str = timestamp_now.strftime("%Y%m%d_%H%M%S")
        iso_time = timestamp_now.isoformat()

        # Đổi tên biến cam_id thành cam_url để tránh nhầm lẫn
        for cam_name, cam_url in CAMERAS.items():
            
            try:
                # GỌI TRỰC TIẾP URL TỪ DICTIONARY, KHÔNG NỐI CHUỖI NỮA
                response = session.get(cam_url, stream=True, timeout=15)
                
                # BƯỚC BẢO VỆ CHỐNG RÁC: Kiểm tra kiểu dữ liệu trả về
                content_type = response.headers.get('Content-Type', '').lower()
                
                # Chỉ xử lý nếu trả về đúng mã 200 VÀ định dạng là hình ảnh
                if response.status_code == 200 and 'image' in content_type:
                    image_filename = f"{cam_name}_{timestamp_str}.jpg"
                    image_filepath = os.path.join(IMAGE_SAVE_DIR, image_filename)
                    
                    with open(image_filepath, 'wb') as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
                            
                    payload = {
                        "ingestion_timestamp": iso_time,
                        "location_name": cam_name,
                        "data_type": "camera_snapshot",
                        "image_path": image_filepath,
                        "file_size_bytes": os.path.getsize(image_filepath)
                    }
                    
                    producer.send(TOPIC, value=payload)
                    logging.info(f"[Camera] Tải THÀNH CÔNG: {cam_name} | Kích thước: {os.path.getsize(image_filepath)} bytes")
                else:
                    logging.error(f"[Camera] BỊ CHẶN hoặc LỖI tại {cam_name}! Server trả về loại dữ liệu: {content_type}")
                    
            except requests.exceptions.RequestException as e:
                logging.error(f"[Camera] Lỗi mạng khi tải ảnh {cam_name}: {e}")

        producer.flush()
        producer.close()
        logging.info("[Camera] Hoàn tất tiến trình Ingestion.")
        
    except Exception as e:
        logging.error(f"Lỗi Kafka Broker: {e}")

if __name__ == "__main__":
    main()