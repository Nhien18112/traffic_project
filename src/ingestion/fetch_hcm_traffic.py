import os
import requests
import json
import logging
from datetime import datetime
from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

KAFKA_BROKER = 'kafka:9092' 
TOPIC = 'raw_camera_events' 
IMAGE_SAVE_DIR = "/opt/airflow/data/raw_cameras"

CAMERAS = {
    "Nga_Tu_Hang_Xanh": "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx?id=5d9ddd49766c880017188c94&bg=black&w=300&h=230",
    "Vong_Xoay_Lang_Cha_Ca": "https://giaothong.hochiminhcity.gov.vn:8007/Render/CameraHandler.ashx?id=5d8cdbdc766c88001718896a&bg=black&w=300&h=230&t=1772643354878",
    "Cau_Kenh_Te": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=63ae7669bfd3d90017e8f0d9&t=1772643466929",
    "Nga_Tu_Thu_Duc": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=56df81d8c062921100c143de&t=1772643506191"
}

def setup_directory():
    if not os.path.exists(IMAGE_SAVE_DIR):
        os.makedirs(IMAGE_SAVE_DIR)

def main():
    setup_directory()
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://giaothong.hochiminhcity.gov.vn/map.aspx"
        })
        session.get("https://giaothong.hochiminhcity.gov.vn/", timeout=10)

        iso_time = datetime.utcnow().isoformat()
        for cam_name, cam_url in CAMERAS.items():
            try:
                response = session.get(cam_url, stream=True, timeout=15)
                if response.status_code == 200:
                    image_filepath = os.path.join(IMAGE_SAVE_DIR, f"{cam_name}.jpg") # Ghi đè ảnh cũ cho nhẹ máy
                    with open(image_filepath, 'wb') as f:
                        for chunk in response.iter_content(1024):
                            f.write(chunk)
                    
                    # Bắn thông báo cho AI Worker biết là có ảnh
                    payload = {
                        "ingestion_timestamp": iso_time,
                        "location_name": cam_name,
                        "image_path": image_filepath
                    }
                    producer.send(TOPIC, value=payload)
                    logging.info(f"[Airflow Shipper] Đã tải ảnh {cam_name} thành công.")
            except Exception as e:
                logging.error(f"[Airflow] Lỗi tải {cam_name}: {e}")

        producer.flush()
        producer.close()
    except Exception as e:
        logging.error(f"Lỗi: {e}")

if __name__ == "__main__":
    main()