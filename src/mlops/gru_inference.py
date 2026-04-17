import os
import json
import logging
import numpy as np
from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [MLOps GRU] - %(message)s')

KAFKA_BROKER = 'kafka:19092'
CONSUME_TOPIC = 'traffic.feature'
PRODUCE_TOPIC = 'traffic.prediction'


def build_congestion_label(pred_speed: float, free_flow_speed: float) -> str:
    if free_flow_speed is None or free_flow_speed <= 0:
        return "Không rõ"
    ratio = pred_speed / free_flow_speed
    if ratio >= 0.8:
        return "Thông thoáng"
    if ratio >= 0.5:
        return "Bình thường"
    if ratio >= 0.25:
        return "Chậm"
    return "Tắc nghẽn"

def main():
    logging.info("Đang khởi tạo Hệ thống MLOps GRU Inference (Scaffold Mode)...")
    
    # // TO-DO: Tích hợp MLflow để lấy GRU weights thật
    # model = load_model('gru_model_v1.h5') 
    
    try:
        logging.info("Đang kết nối Kafka Consumer Group...")
        consumer = KafkaConsumer(
            CONSUME_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else {},
            auto_offset_reset='latest',
            max_poll_records=32, # Hệ thống Dynamic Mini-batching (Giảm overhead Model)
            group_id='gru_inference_group'
        )
        
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        logging.info("Sẵn sàng lắng nghe Tensor Feature Vectors...")

        for message in consumer:
            feature_data = message.value
            location = feature_data.get('location_name')
            prediction_time = feature_data.get('time')
            current_speed = feature_data.get('current_speed', feature_data.get('currentSpeed'))
            free_flow_speed = feature_data.get('free_flow_speed', feature_data.get('freeFlowSpeed'))
            
            # Data contract tối thiểu giữa Spark Feature Store và GRU inference.
            if not location or prediction_time is None or current_speed is None or free_flow_speed is None:
                logging.warning(
                    "Bỏ qua payload không hợp lệ theo contract: thiếu location/time/current_speed/free_flow_speed"
                )
                continue
                
            # 1. Trích xuất features thành mảng Tensor (Giả lập cho Code chạy)
            # Mục đích: Định hướng rõ luồng input cho Model Engineer sau này
            # tensor_input = np.array([feature_data['currentSpeed'], feature_data['motorcycle_count'], ...])
            
            # 2. Chạy GRU Inference Queue (Giả lập kết quả)
            # prediction = model.predict(tensor_input)
            current_speed = float(current_speed)
            free_flow_speed = float(free_flow_speed)

            # Multi-horizon prediction (5m / 10m / 15m)
            predicted_speed_5m = max(0.0, current_speed * 0.95)
            predicted_speed_10m = max(0.0, current_speed * 0.90)
            predicted_speed_15m = max(0.0, current_speed * 0.85)

            label_5m = build_congestion_label(predicted_speed_5m, free_flow_speed)
            label_10m = build_congestion_label(predicted_speed_10m, free_flow_speed)
            label_15m = build_congestion_label(predicted_speed_15m, free_flow_speed)
            
            # 3. Ghi kết quả vào Sink
            prediction_payload = {
                "prediction_timestamp": prediction_time,
                "location_name": location,
                "predicted_speed": round(predicted_speed_5m, 2),
                "predicted_congestion_label": label_5m,
                "predicted_speed_5m": round(predicted_speed_5m, 2),
                "predicted_speed_10m": round(predicted_speed_10m, 2),
                "predicted_speed_15m": round(predicted_speed_15m, 2),
                "predicted_congestion_label_5m": label_5m,
                "predicted_congestion_label_10m": label_10m,
                "predicted_congestion_label_15m": label_15m,
                "model_version": "gru_v1.0.0_poc"
            }
            
            producer.send(PRODUCE_TOPIC, key=location.encode('utf-8'), value=prediction_payload)
            logging.info(
                f"Đã Push Dự báo {location}: 5m={predicted_speed_5m:.1f}, 10m={predicted_speed_10m:.1f}, 15m={predicted_speed_15m:.1f} km/h"
            )
            
    except Exception as e:
        logging.error(f"System Error in Distributed ML Inference: {e}")

if __name__ == "__main__":
    main()
