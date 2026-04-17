import os
import json
import logging
import cv2
import requests
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from ultralytics import YOLO

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [AI Worker] - %(message)s')

KAFKA_BROKER = 'kafka:19092'
CONSUME_TOPIC = 'camera.raw'
PRODUCE_TOPIC = 'camera.processed'

logging.info("Đang nạp mô hình YOLOv8...")
model = YOLO('yolov8s.pt')
VEHICLE_CLASSES = [1, 2, 3, 5, 7] # 1: Bicycle, 2: Car, 3: Motorcycle, 5: Bus, 7: Truck

def main():
    logging.info("Đang kết nối Kafka Consumer Group...")
    consumer = KafkaConsumer(
        CONSUME_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        max_poll_records=10, # Distributed Consumer Queue (Batch Polling)
        group_id='yolo_vision_group'
    )
    
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    logging.info("AI Worker đã sẵn sàng chờ Event từ Camera.raw...")

    for message in consumer:
        event = message.value
        image_url = event.get('image_url')
        location = event.get('location_name')
        
        if image_url:
            try:
                # 1. Tải ảnh từ MinIO Data Lake vào RAM
                resp = requests.get(image_url, timeout=5)
                if resp.status_code == 200:
                    image_array = np.asarray(bytearray(resp.content), dtype="uint8")
                    img = cv2.imdecode(image_array, cv2.IMREAD_COLOR)
                    
                    if img is not None:
                        # 2. Chạy AI đếm xe 
                        results = model(img, classes=VEHICLE_CLASSES, conf=0.15, verbose=False)
                        counts = {"car": 0, "motorcycle": 0, "bus": 0, "truck": 0}
                        
                        for r in results:
                            for box in r.boxes:
                                cls_id = int(box.cls[0])
                                if cls_id == 2: counts["car"] += 1
                                elif cls_id in [1, 3]: counts["motorcycle"] += 1 
                                elif cls_id == 5: counts["bus"] += 1
                                elif cls_id == 7: counts["truck"] += 1
                        
                        # 3. Push nhãn đã xử lý vào Camera.processed
                        traffic_payload = {
                            "ingestion_timestamp": event["ingestion_timestamp"],
                            "location_name": location,
                            "motorcycle_count": counts["motorcycle"],
                            "car_count": counts["car"],
                            "bus_truck_count": counts["bus"] + counts["truck"]
                        }
                        
                        producer.send(PRODUCE_TOPIC, key=location.encode('utf-8'), value=traffic_payload)
                        logging.info(f"Đã xử lý {location} | Xe máy: {counts['motorcycle']} | Ô tô: {counts['car']}")
                        
            except Exception as e:
                logging.error(f"Lỗi Inference YOLO tại {image_url}: {e}")

if __name__ == "__main__":
    main()