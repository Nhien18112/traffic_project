import os
import logging
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, lit, coalesce, to_json, struct
from psycopg2.extras import execute_values
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TrafficStreamCQRS")

POSTGRES_JDBC_URL = os.environ.get("POSTGRES_JDBC_URL", "")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_URL = os.environ.get("POSTGRES_URL")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "traffic.dlq")
KAFKA_STARTING_OFFSETS = os.environ.get("KAFKA_STARTING_OFFSETS", "earliest")

# Không cần Logic SNI tự động tìm Project_ID nữa vì đã chạy bằng Docker Postgres Local


def ensure_quality_metrics_table():
    create_sql = """
        CREATE TABLE IF NOT EXISTS pipeline_quality_metrics (
            id BIGSERIAL PRIMARY KEY,
            event_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            pipeline TEXT NOT NULL,
            epoch_id BIGINT NOT NULL,
            total_count INTEGER NOT NULL,
            valid_count INTEGER NOT NULL,
            invalid_count INTEGER NOT NULL,
            dropped_count INTEGER NOT NULL,
            dlq_count INTEGER NOT NULL,
            rows_written INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_pipeline_quality_event_time
            ON pipeline_quality_metrics (event_time DESC);

        CREATE INDEX IF NOT EXISTS idx_pipeline_quality_pipeline_time
            ON pipeline_quality_metrics (pipeline, event_time DESC);
    """

    with psycopg2.connect(POSTGRES_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(create_sql)
        conn.commit()


def write_quality_metric(pipeline, epoch_id, total_count, valid_count, invalid_count, dropped_count, dlq_count, rows_written):
    insert_sql = """
        INSERT INTO pipeline_quality_metrics (
            pipeline,
            epoch_id,
            total_count,
            valid_count,
            invalid_count,
            dropped_count,
            dlq_count,
            rows_written
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    with psycopg2.connect(POSTGRES_URL) as conn:
        with conn.cursor() as cur:
            cur.execute(
                insert_sql,
                (
                    pipeline,
                    int(epoch_id),
                    int(total_count),
                    int(valid_count),
                    int(invalid_count),
                    int(dropped_count),
                    int(dlq_count),
                    int(rows_written),
                ),
            )
        conn.commit()

def process_traffic_weather_batch(df, epoch_id):
    row_count = df.count()
    if row_count > 0:
        try:
            required_cond = (
                col("time").isNotNull()
                & col("location_name").isNotNull()
                & col("currentSpeed").isNotNull()
                & col("freeFlowSpeed").isNotNull()
            )

            # Contract bắt buộc cho bản ghi realtime dùng cho cả serving và model.
            valid_df = df.where(required_cond).dropDuplicates(["location_name", "time"])
            invalid_df = df.where(~required_cond)

            valid_count = valid_df.count()
            dropped_count = row_count - valid_count
            invalid_count = invalid_df.count()

            if invalid_count > 0:
                dlq_payload_df = invalid_df.select(
                    coalesce(col("location_name"), lit("unknown")).alias("key"),
                    to_json(
                        struct(
                            lit("traffic.feature").alias("source"),
                            lit("missing_required_fields").alias("reason"),
                            lit(int(epoch_id)).alias("epoch_id"),
                            col("time").cast("string").alias("time"),
                            col("location_name"),
                            col("currentSpeed"),
                            col("freeFlowSpeed"),
                            col("incident_count"),
                        )
                    ).alias("value"),
                )

                dlq_payload_df.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:19092") \
                    .option("topic", DLQ_TOPIC) \
                    .save()

            if valid_count == 0:
                logger.warning(
                    f"Batch FAST {epoch_id}: không có record hợp lệ theo data contract, bỏ qua batch."
                )
                return

            # 1. Ghi ra Analytics Sink: MinIO (Parquet)
            valid_df.write \
                .mode("append") \
                .parquet("s3a://raw-data-lake/feature_lake/")
            
            # 2. Upsert Serving DB để idempotent khi replay/checkpoint recovery.
            rows = [
                (
                    r["time"],
                    r["location_name"],
                    r["weather_condition"],
                    r["temperature"],
                    r["humidity"],
                    r["wind_speed"],
                    r["visibility"],
                    r["currentSpeed"],
                    r["freeFlowSpeed"],
                    r["incident_count"],
                )
                for r in valid_df.collect()
            ]

            upsert_query = """
                INSERT INTO realtime_traffic_weather (
                    "time", location_name, weather_condition, temperature, humidity, wind_speed, visibility,
                    "currentSpeed", "freeFlowSpeed", incident_count
                )
                VALUES %s
                ON CONFLICT (location_name, "time") DO UPDATE SET
                    weather_condition = EXCLUDED.weather_condition,
                    temperature = EXCLUDED.temperature,
                    humidity = EXCLUDED.humidity,
                    wind_speed = EXCLUDED.wind_speed,
                    visibility = EXCLUDED.visibility,
                    "currentSpeed" = EXCLUDED."currentSpeed",
                    "freeFlowSpeed" = EXCLUDED."freeFlowSpeed",
                    incident_count = EXCLUDED.incident_count;
            """

            with psycopg2.connect(POSTGRES_URL) as conn:
                with conn.cursor() as cur:
                    execute_values(cur, upsert_query, rows)
                conn.commit()
            
            # 3. Ghi ra Kafka (Feature Store) theo schema contract thống nhất train/inference.
            model_features_df = valid_df.select(
                col("time"),
                col("location_name"),
                col("currentSpeed").alias("current_speed"),
                col("freeFlowSpeed").alias("free_flow_speed"),
                col("incident_count"),
                col("weather_condition"),
                col("temperature"),
                col("humidity"),
                col("wind_speed"),
                col("visibility"),
                # Backward-compatible fields cho consumer cũ.
                col("currentSpeed"),
                col("freeFlowSpeed"),
            )

            model_features_df.selectExpr("location_name AS key", "to_json(struct(*)) AS value") \
                .write \
                .format("kafka") \
                .option("kafka.bootstrap.servers", "kafka:19092") \
                .option("topic", "traffic.feature") \
                .save()

            write_quality_metric(
                pipeline="traffic_fast",
                epoch_id=epoch_id,
                total_count=row_count,
                valid_count=valid_count,
                invalid_count=invalid_count,
                dropped_count=dropped_count,
                dlq_count=invalid_count,
                rows_written=valid_count,
            )
                
            logger.info(
                f"Batch FAST {epoch_id}: valid={valid_count}, invalid={invalid_count}, dropped={dropped_count}, đã đẩy CQRS Sink (Postgres+MinIO+Kafka)."
            )
        except Exception as e:
            logger.error(f"Lỗi Traffic Sink tại Batch {epoch_id}: {str(e)}")

def process_camera_batch(df, epoch_id):
    row_count = df.count()
    if row_count > 0:
        try:
            required_cond = (
                col("ingestion_timestamp").isNotNull()
                & col("location_name").isNotNull()
            )

            valid_camera_df = df.where(required_cond)
            invalid_camera_df = df.where(~required_cond)
            invalid_count = invalid_camera_df.count()

            if invalid_count > 0:
                dlq_payload_df = invalid_camera_df.select(
                    coalesce(col("location_name"), lit("unknown")).alias("key"),
                    to_json(
                        struct(
                            lit("camera.processed").alias("source"),
                            lit("missing_required_fields").alias("reason"),
                            lit(int(epoch_id)).alias("epoch_id"),
                            col("ingestion_timestamp").cast("string").alias("time"),
                            col("location_name"),
                            col("motorcycle_count"),
                            col("car_count"),
                            col("bus_truck_count"),
                        )
                    ).alias("value"),
                )

                dlq_payload_df.write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "kafka:19092") \
                    .option("topic", DLQ_TOPIC) \
                    .save()

            camera_for_db = valid_camera_df.select(
                col("ingestion_timestamp").alias("time"),
                col("location_name"),
                col("motorcycle_count"),
                col("car_count"),
                col("bus_truck_count")
            ).dropDuplicates(["time", "location_name"])

            rows = [
                (
                    r["time"],
                    r["location_name"],
                    r["motorcycle_count"],
                    r["car_count"],
                    r["bus_truck_count"],
                )
                for r in camera_for_db.collect()
            ]

            if not rows:
                write_quality_metric(
                    pipeline="camera_slow",
                    epoch_id=epoch_id,
                    total_count=row_count,
                    valid_count=0,
                    invalid_count=invalid_count,
                    dropped_count=row_count,
                    dlq_count=invalid_count,
                    rows_written=0,
                )
                return

            upsert_query = """
                INSERT INTO realtime_camera ("time", location_name, motorcycle_count, car_count, bus_truck_count)
                VALUES %s
                ON CONFLICT (location_name, "time") DO UPDATE SET
                    motorcycle_count = EXCLUDED.motorcycle_count,
                    car_count = EXCLUDED.car_count,
                    bus_truck_count = EXCLUDED.bus_truck_count;
            """

            with psycopg2.connect(POSTGRES_URL) as conn:
                with conn.cursor() as cur:
                    execute_values(cur, upsert_query, rows)
                conn.commit()

            write_quality_metric(
                pipeline="camera_slow",
                epoch_id=epoch_id,
                total_count=row_count,
                valid_count=len(rows),
                invalid_count=invalid_count,
                dropped_count=row_count - len(rows),
                dlq_count=invalid_count,
                rows_written=len(rows),
            )

            logger.info(
                f"Batch SLOW-CAMERA {epoch_id}: valid={len(rows)}, invalid={invalid_count}, đã đẩy records Camera sang Database độc lập."
            )
        except Exception as e:
            logger.error(f"Lỗi Camera Sink tại Batch {epoch_id}: {str(e)}")

def main():
    if not all([POSTGRES_JDBC_URL, POSTGRES_USER, POSTGRES_PASSWORD]):
        logger.error("Cấu hình PostgreSQL thiếu! Kiểm tra file .env")
        return

    spark = SparkSession.builder \
        .appName("TrafficFeaturePipeline") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    ensure_quality_metrics_table()

    spark.sparkContext.setLogLevel("ERROR")

    # --- ĐỊNH NGHĨA SCHEMA KAFKA ---
    tomtom_data_schema = StructType([
        StructField("flowSegmentData", StructType([
            StructField("currentSpeed", DoubleType(), True),
            StructField("freeFlowSpeed", DoubleType(), True)
        ]), True)
    ])
    
    traffic_schema = StructType([
        StructField("location_name", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("tomtom_data", tomtom_data_schema, True),
        StructField("incident_count", IntegerType(), True)
    ])

    weather_schema = StructType([
        StructField("location_name", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("weather_main", StringType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("visibility", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])

    camera_schema = StructType([
        StructField("location_name", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("motorcycle_count", DoubleType(), True),
        StructField("car_count", DoubleType(), True),
        StructField("bus_truck_count", DoubleType(), True)
    ])

    def read_kafka_topic(topic):
        return spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:19092") \
            .option("subscribe", topic) \
            .option("startingOffsets", KAFKA_STARTING_OFFSETS) \
            .option("failOnDataLoss", "false") \
            .load()

    df_traffic_raw = read_kafka_topic("traffic.raw")
    df_weather_raw = read_kafka_topic("weather.raw")
    df_camera_raw = read_kafka_topic("camera.processed")

    df_traffic = df_traffic_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), traffic_schema).alias("data")).select("data.*") \
        .withWatermark("ingestion_timestamp", "1 minute")

    df_weather = df_weather_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), weather_schema).alias("data")).select("data.*") \
        .withWatermark("ingestion_timestamp", "1 minute")

    # Camera watermark có thể trễ hơn nếu cần, vì nó xử lý chậm
    df_camera = df_camera_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), camera_schema).alias("data")).select("data.*") \
        .withWatermark("ingestion_timestamp", "1 minute")

    # MẠNG LƯỚI STREAM 1 (FAST STREAM): GIAO THÔNG VÀ THỜI TIẾT
    traffic_weather_joined = df_traffic.alias("t").join(
        df_weather.alias("w"),
        expr("""
            t.location_name = w.location_name AND
            w.ingestion_timestamp >= t.ingestion_timestamp - INTERVAL 1 MINUTE AND
            w.ingestion_timestamp <= t.ingestion_timestamp + INTERVAL 1 MINUTE
        """),
        "inner"
    ).select(
        col("t.ingestion_timestamp").alias("time"),
        col("t.location_name"),
        col("w.weather_main").alias("weather_condition"),
        col("w.temperature"),
        col("w.humidity"),
        col("w.wind_speed"),
        col("w.visibility"),
        col("t.tomtom_data.flowSegmentData.currentSpeed").alias("currentSpeed"), 
        col("t.tomtom_data.flowSegmentData.freeFlowSpeed").alias("freeFlowSpeed"),
        col("t.incident_count")
    )

    query_traffic = traffic_weather_joined.writeStream \
        .foreachBatch(process_traffic_weather_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://raw-data-lake/checkpoints/traffic_pipeline") \
        .start()

    # MẠNG LƯỚI STREAM 2 (SLOW STREAM): XỬ LÝ ẢNH CAMERA ĐỘC LẬP
    query_camera = df_camera.writeStream \
        .foreachBatch(process_camera_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "s3a://raw-data-lake/checkpoints/camera_pipeline") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()