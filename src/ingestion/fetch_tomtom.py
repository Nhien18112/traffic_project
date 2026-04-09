import os
import requests
import json
import io
from datetime import datetime
from kafka import KafkaProducer
from minio import Minio

KAFKA_BROKER = 'kafka:9092' 
TOPIC = 'raw_tomtom_traffic'
API_KEY = os.getenv("TOMTOM_API_KEY")

LOCATIONS = {
    "Nga_Tu_Hang_Xanh": {"lat": "10.8015", "lon": "106.7111"},
    "Vong_Xoay_Lang_Cha_Ca": {"lat": "10.8023", "lon": "106.6603"},
    "Cau_Kenh_Te": {"lat": "10.7523", "lon": "106.6972"},
    "Nga_Tu_Thu_Duc": {"lat": "10.8504", "lon": "106.7716"},
    "Nut_Giao_Ly_Thuong_Kiet_3Thang2": {"lat": "10.763875", "lon": "106.660007"},
    "Nut_Giao_Bac_Hai_Thanh_Thai": {"lat": "10.78082", "lon": "106.65883"},
    "Nga_Tu_Thanh_Thai_To_Hien_Thanh": {"lat": "10.77639", "lon": "106.66353"},
    "Nut_Giao_3Thang2_Thanh_Thai": {"lat": "10.76778", "lon": "106.66702"},
    "Nut_Giao_Su_Van_Hanh_To_Hien_Thanh": {"lat": "10.77795", "lon": "106.66557"},
    "Nut_Giao_3Thang2_Su_Van_Hanh": {"lat": "10.76962", "lon": "106.67076"},
    "Nut_Giao_CMT8_Dien_Bien_Phu": {"lat": "10.77676", "lon": "106.68371"},
    "Nut_Giao_Cao_Thang_Dien_Bien_Phu": {"lat": "10.77276", "lon": "106.679"},
    "Nut_Giao_3Thang2_Le_Hong_Phong": {"lat": "10.77091", "lon": "106.67311"},
    "Nut_Giao_3Thang2_Cao_Thang": {"lat": "10.77381", "lon": "106.67778"},
    "Nut_Giao_3Thang2_Ly_Thai_To": {"lat": "10.7681", "lon": "106.66803"},
    "Bung_binh_Ly_Thai_To": {"lat": "10.76776", "lon": "106.67418"},
    "Nut_Giao_Truong_Chinh_Au_Co": {"lat": "10.80184", "lon": "106.63674"},
    "Nut_Giao_3Thang2_Le_Dai_Hanh": {"lat": "10.76226", "lon": "106.65707"},
    "Nut_Giao_Lac_Long_Quan_Ly_Thuong_Kiet": {"lat": "10.79034", "lon": "106.65236"},
    "Nut_Giao_Lac_Long_Quan_Au_Co": {"lat": "10.77478", "lon": "106.64798"},
    "Nut_Giao_Lu_Gia_Nguyen_Thi_Nho": {"lat": "10.77115", "lon": "106.65292"},
    "Nut_Giao_Au_Co_Nguyen_Thi_Nho": {"lat": "10.76906", "lon": "106.65226"},
    "Duong_Au_Co": {"lat": "10.78938", "lon": "106.6404"},
    "Nut_Giao_Cong_Hoa_Hoang_Hoa_Tham": {"lat": "10.80184", "lon": "106.64742"},
    "Nut_Giao_Truong_Chinh_Hoang_Hoa_Tham": {"lat": "10.79637", "lon": "106.64694"}
}

def main():
    if not API_KEY:
        print("LỖI: Không tìm thấy TOMTOM_API_KEY.")
        return

    try:
        # 1. Khởi tạo Kafka Producer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            retries=3 
        )
        
        # 2. Khởi tạo MinIO Client
        minio_client = Minio(
            "minio:9000",
            access_key="minioadmin",
            secret_key="minioadmin",
            secure=False
        )
        
        timestamp_now = datetime.utcnow()
        timestamp_str = timestamp_now.strftime("%Y%m%d_%H%M%S")
        iso_time = timestamp_now.isoformat()

        for loc_name, coords in LOCATIONS.items():
            lat, lon = coords["lat"], coords["lon"]
            url = f"https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json?key={API_KEY}&point={lat},{lon}"
            
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    
                    enriched_payload = {
                        "ingestion_timestamp": iso_time,
                        "location_name": loc_name,
                        "latitude": lat,
                        "longitude": lon,
                        "tomtom_data": data 
                    }
                    
                    # --- LƯU RAW VÀO MINIO (BRONZE LAYER) ---
                    json_bytes = json.dumps(enriched_payload, ensure_ascii=False).encode('utf-8')
                    file_name = f"tomtom/{loc_name}_{timestamp_str}.json"
                    
                    minio_client.put_object(
                        bucket_name="raw-data-lake",
                        object_name=file_name,
                        data=io.BytesIO(json_bytes),
                        length=len(json_bytes),
                        content_type="application/json"
                    )
                    
                    # --- ĐẨY VÀO KAFKA CHO SPARK XỬ LÝ ---
                    producer.send(TOPIC, value=enriched_payload)
                    
            except Exception as e:
                print(f"Lỗi tại {loc_name}: {e}")

        producer.flush() 
        producer.close()
        
    except Exception as e:
        print(f"Lỗi hệ thống: {e}")

if __name__ == "__main__":
    main()