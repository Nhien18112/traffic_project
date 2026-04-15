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
    "Nga_Tu_Thu_Duc": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=56df81d8c062921100c143de&t=1772643506191",
    
    "Nut_Giao_Ly_Thuong_Kiet_3Thang2":"https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=63ae7af4bfd3d90017e8f32c&t=1776259938762",
    "Nga_Tu_Thanh_Thai_To_Hien_Thanh": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=66b1c370779f7400186740b3&t=1776260378130",
    "Nut_Giao_3Thang2_Thanh_Thai": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=66b1c34d779f74001867409e&t=1776260060091",
    "Nut_Giao_Su_Van_Hanh_To_Hien_Thanh": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=63ae7a74bfd3d90017e8f2c7&t=1776260435250",
    "Nut_Giao_3Thang2_Su_Van_Hanh": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=63ae7a50bfd3d90017e8f2b2&t=1776260174568",
    "Nut_Giao_CMT8_Dien_Bien_Phu": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=5deb576d1dc17d7c5515acf2&t=1776260462061",
    "Nut_Giao_Cao_Thang_Dien_Bien_Phu": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=63ae7a9cbfd3d90017e8f303&t=1776260492827",
    "Nut_Giao_3Thang2_Le_Hong_Phong": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?",
    "Nut_Giao_3Thang2_Cao_Thang": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=5deb576d1dc17d7c5515acf8&t=1776260232439",
    "Nut_Giao_3Thang2_Ly_Thai_To": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=5deb576d1dc17d7c5515acf3&t=1776260146906",
    "Bung_binh_Ly_Thai_To": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=5deb576d1dc17d7c5515acf5&t=1776260124934",
    "Nut_Giao_Truong_Chinh_Au_Co": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=56df807bc062921100c143da&t=1776259350759",
    "Nut_Giao_3Thang2_Le_Dai_Hanh": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=63ae7c12bfd3d90017e8f3c0&t=1776259842764",
    "Nut_Giao_Lac_Long_Quan_Ly_Thuong_Kiet": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=5d8cdc57766c88001718896e&t=1776259687770",
    "Nut_Giao_Lac_Long_Quan_Au_Co": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=5d8cdc9d766c880017188970&t=1776259560762",
    "Nut_Giao_Au_Co_Nguyen_Thi_Nho": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=5d8cdd26766c880017188974&t=1776259602763",
    "Duong_Au_Co": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=6623f0246f998a001b252797&t=1776259447760",
    "Nut_Giao_Cong_Hoa_Hoang_Hoa_Tham": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=58ad6214bd82540010390be2&t=1776259477768",
    "Nut_Giao_Truong_Chinh_Hoang_Hoa_Tham": "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=5deb576d1dc17d7c5515ad09&t=1776259504765",
    "Nut_Giao_Truong_Chinh_Cong_Hoa" : "https://giaothong.hochiminhcity.gov.vn/render/ImageHandler.ashx?id=586e1f18f9fab7001111b0a5&t=1776259115766"
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