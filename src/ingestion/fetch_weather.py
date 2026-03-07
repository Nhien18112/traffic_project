import os
import requests
import json
import io
from datetime import datetime
from kafka import KafkaProducer
from minio import Minio

KAFKA_BROKER = 'kafka:9092' 
TOPIC = 'raw_weather_data'
API_KEY = os.getenv("WEATHER_API_KEY")

LOCATIONS = {
    "Nga_Tu_Hang_Xanh": {"lat": "10.8015", "lon": "106.7111"},
    "Vong_Xoay_Lang_Cha_Ca": {"lat": "10.8023", "lon": "106.6603"},
    "Cau_Kenh_Te": {"lat": "10.7523", "lon": "106.6972"},
    "Nga_Tu_Thu_Duc": {"lat": "10.8504", "lon": "106.7716"}
}

def main():
    if not API_KEY:
        print("LỖI BẢO MẬT: Không tìm thấy WEATHER_API_KEY trong file .env!")
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
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
            
            try:
                response = requests.get(url, timeout=10)
                
                if response.status_code == 200:
                    data = response.json()
                    
                    enriched_payload = {
                        "ingestion_timestamp": iso_time,
                        "location_name": loc_name,
                        "latitude": lat,
                        "longitude": lon,
                        "temperature": data.get("main", {}).get("temp"),
                        "weather_main": data.get("weather", [{}])[0].get("main"),
                        "weather_data": data 
                    }
                    
                    # --- LƯU RAW VÀO MINIO (BRONZE LAYER) ---
                    json_bytes = json.dumps(enriched_payload, ensure_ascii=False).encode('utf-8')
                    file_name = f"weather/{loc_name}_{timestamp_str}.json"
                    
                    minio_client.put_object(
                        bucket_name="raw-data-lake",
                        object_name=file_name,
                        data=io.BytesIO(json_bytes),
                        length=len(json_bytes),
                        content_type="application/json"
                    )
                    
                    # --- ĐẨY VÀO KAFKA CHO SPARK XỬ LÝ ---
                    producer.send(TOPIC, value=enriched_payload)
                    
                else:
                    print(f"Lỗi {response.status_code} tại {loc_name}: {response.text}")
                    
            except Exception as e:
                print(f"Lỗi mạng tại {loc_name}: {e}")

        producer.flush()
        producer.close()
        
    except Exception as e:
        print(f"Lỗi hệ thống: {e}")

if __name__ == "__main__":
    main()