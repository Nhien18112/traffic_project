import os
import json
import logging
import cv2  # <-- THÊM THƯ VIỆN NÀY ĐỂ XỬ LÝ ẢNH
from kafka import KafkaConsumer, KafkaProducer
from ultralytics import YOLO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [AI Worker] - %(message)s')

KAFKA_BROKER = 'kafka:9092'
CONSUME_TOPIC = 'raw_camera_events'
PRODUCE_TOPIC = 'raw_camera_traffic'

# Đường dẫn lưu ảnh Debug 
DEBUG_SAVE_DIR = "/opt/airflow/data/debug_cameras"

logging.info("Đang nạp mô hình YOLOv8...")
model = YOLO('yolov8s.pt') 
VEHICLE_CLASSES = [1, 2, 3, 5, 7] # 1: Bicycle, 2: Car, 3: Motorcycle, 5: Bus, 7: Truck

def setup_debug_dir():
    if not os.path.exists(DEBUG_SAVE_DIR):
        os.makedirs(DEBUG_SAVE_DIR)

def main():
    setup_debug_dir()
    
    consumer = KafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    logging.info("AI Worker đã sẵn sàng. Đang chờ ảnh từ Airflow...")

    for message in consumer:
        event = message.value
        image_path = event.get('image_path')
        location = event.get('location_name')
        
        if image_path and os.path.exists(image_path):
            try:
                # 1. Chạy AI đếm xe 
                results = model(image_path, classes=VEHICLE_CLASSES, conf=0.15, verbose=False)
                counts = {"car": 0, "motorcycle": 0, "bus": 0, "truck": 0}
                
                for r in results:
                    annotated_frame = r.plot()
                    debug_filepath = os.path.join(DEBUG_SAVE_DIR, f"debug_{location}.jpg")
                    cv2.imwrite(debug_filepath, annotated_frame)
                    
                    for box in r.boxes:
                        cls_id = int(box.cls[0])
                        if cls_id == 2: counts["car"] += 1
                        elif cls_id in [1, 3]: counts["motorcycle"] += 1 
                        elif cls_id == 5: counts["bus"] += 1
                        elif cls_id == 7: counts["truck"] += 1
                
                # 2. Đóng gói kết quả gửi cho Spark
                traffic_payload = {
                    "ingestion_timestamp": event["ingestion_timestamp"],
                    "location_name": location,
                    "motorcycle_count": counts["motorcycle"],
                    "car_count": counts["car"],
                    "bus_truck_count": counts["bus"] + counts["truck"]
                }
                
                producer.send(PRODUCE_TOPIC, value=traffic_payload)
                logging.info(f"Đã đếm xong {location} | Xe máy: {counts['motorcycle']} | Ô tô: {counts['car']}")
                
            except Exception as e:
                logging.error(f"Lỗi AI xử lý ảnh {image_path}: {e}")
        else:
            logging.warning(f"Không tìm thấy file ảnh tại: {image_path}")

if __name__ == "__main__":
    main()