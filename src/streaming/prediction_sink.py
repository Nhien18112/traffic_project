import os
import json
import logging
from kafka import KafkaConsumer
import psycopg2

logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Prediction Sink] - %(message)s')

KAFKA_BROKER = 'kafka:19092'
CONSUME_TOPIC = 'traffic.prediction'

POSTGRES_URL = os.environ.get("POSTGRES_URL")


def ensure_prediction_columns(conn):
    ddl = """
        ALTER TABLE realtime_traffic_weather
            ADD COLUMN IF NOT EXISTS predicted_speed_5m DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS predicted_speed_10m DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS predicted_speed_15m DOUBLE PRECISION,
            ADD COLUMN IF NOT EXISTS predicted_congestion_label_5m TEXT,
            ADD COLUMN IF NOT EXISTS predicted_congestion_label_10m TEXT,
            ADD COLUMN IF NOT EXISTS predicted_congestion_label_15m TEXT;
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

def main():
    if not POSTGRES_URL:
        logging.error("Cấu hình POSTGRES_URL thiếu! Dừng Prediction Sink.")
        return

    try:
        consumer = KafkaConsumer(
            CONSUME_TOPIC,
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')) if m else {},
            auto_offset_reset='latest',
            group_id='prediction_postgres_sink'
        )
        logging.info("Prediction Sink đã sẵn sàng chờ Event từ traffic.prediction...")
        
        # Thiết lập kết nối
        conn = psycopg2.connect(POSTGRES_URL)
        ensure_prediction_columns(conn)
        cur = conn.cursor()

        for message in consumer:
            pred_data = message.value
            location = pred_data.get('location_name')
            p_time = pred_data.get('prediction_timestamp')
            p_speed = pred_data.get('predicted_speed')
            p_label = pred_data.get('predicted_congestion_label')
            p_speed_5m = pred_data.get('predicted_speed_5m', p_speed)
            p_speed_10m = pred_data.get('predicted_speed_10m')
            p_speed_15m = pred_data.get('predicted_speed_15m')
            p_label_5m = pred_data.get('predicted_congestion_label_5m', p_label)
            p_label_10m = pred_data.get('predicted_congestion_label_10m')
            p_label_15m = pred_data.get('predicted_congestion_label_15m')
            version = pred_data.get('model_version')

            if not location or not p_time:
                continue
                
            # Chỉ update vào row realtime đã tồn tại để tránh sinh prediction-only row thiếu feature.
            query = """
                WITH target AS (
                    SELECT "time"
                    FROM realtime_traffic_weather
                    WHERE location_name = %s
                      AND "time" BETWEEN (%s::timestamptz - INTERVAL '2 minutes')
                                     AND (%s::timestamptz + INTERVAL '2 minutes')
                    ORDER BY ABS(EXTRACT(EPOCH FROM ("time" - %s::timestamptz))) ASC
                    LIMIT 1
                )
                UPDATE realtime_traffic_weather t
                SET
                    predicted_speed = %s,
                    predicted_congestion_label = %s,
                    predicted_speed_5m = %s,
                    predicted_speed_10m = %s,
                    predicted_speed_15m = %s,
                    predicted_congestion_label_5m = %s,
                    predicted_congestion_label_10m = %s,
                    predicted_congestion_label_15m = %s,
                    model_version = %s
                FROM target
                WHERE t.location_name = %s
                  AND t."time" = target."time";
            """
            
            try:
                cur.execute(
                    query,
                    (
                        location,
                        p_time,
                        p_time,
                        p_time,
                        p_speed,
                        p_label,
                        p_speed_5m,
                        p_speed_10m,
                        p_speed_15m,
                        p_label_5m,
                        p_label_10m,
                        p_label_15m,
                        version,
                        location,
                    ),
                )
                conn.commit()
                if cur.rowcount == 0:
                    logging.warning(
                        f"Bỏ qua prediction của {location} tại {p_time}: chưa tìm thấy record realtime tương ứng."
                    )
                else:
                    logging.info(f"Đã cập nhật prediction cho {location}")
            except Exception as e:
                conn.rollback()
                logging.error(f"Lỗi Postgres: {e}")

    except Exception as e:
        logging.error(f"Postgres Sink Kafka Error: {e}")

if __name__ == "__main__":
    main()
