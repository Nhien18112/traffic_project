import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Cấu hình Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("TrafficStream")

NEON_JDBC_URL = os.environ.get("NEON_JDBC_URL")
NEON_USER = os.environ.get("NEON_USER")
NEON_PASSWORD = os.environ.get("NEON_PASSWORD")

def process_batch(df, epoch_id):
    """
    Xử lý ghi dữ liệu theo lô nhỏ (Micro-batch)
    """
    row_count = df.count()
    if row_count > 0:
        try:
            # Ghi dữ liệu lên Neon
            df.write \
                .format("jdbc") \
                .option("url", NEON_JDBC_URL) \
                .option("driver", "org.postgresql.Driver") \
                .option("dbtable", "realtime_traffic_weather") \
                .option("user", NEON_USER) \
                .option("password", NEON_PASSWORD) \
                .mode("append") \
                .save()
            
            logger.info(f"Batch {epoch_id}: Đã đẩy {row_count} dòng lên Cloud.")
        except Exception as e:
            logger.error(f"Lỗi tại Batch {epoch_id}: {str(e)}")

def main():
    if not all([NEON_JDBC_URL, NEON_USER, NEON_PASSWORD]):
        logger.error("Cấu hình Neon thiếu! Kiểm tra file .env")
        return

    spark = SparkSession.builder \
        .appName("TrafficWeatherStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    # Tắt các log thừa thãi của chính Spark
    spark.sparkContext.setLogLevel("ERROR") 

    # --- ĐỊNH NGHĨA SCHEMA ---
    tomtom_data_schema = StructType([
        StructField("flowSegmentData", StructType([
            StructField("currentSpeed", DoubleType(), True),
            StructField("freeFlowSpeed", DoubleType(), True) 
        ]), True)
    ])
    
    traffic_schema = StructType([
        StructField("location_name", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("tomtom_data", tomtom_data_schema, True) 
    ])

    weather_schema = StructType([
        StructField("location_name", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weather_main", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])

    camera_schema = StructType([
        StructField("location_name", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("motorcycle_count", DoubleType(), True),
        StructField("car_count", DoubleType(), True),
        StructField("bus_truck_count", DoubleType(), True)
    ])

# --- ĐỌC STREAM TỪ KAFKA ---
    def read_kafka_topic(topic):
        return spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .load()

    df_traffic_raw = read_kafka_topic("raw_tomtom_traffic")
    df_weather_raw = read_kafka_topic("raw_weather_data")
    df_camera_raw = read_kafka_topic("raw_camera_traffic")

    # --- PARSE JSON & WATERMARK ---
    df_traffic = df_traffic_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), traffic_schema).alias("data")).select("data.*") \
        .withWatermark("ingestion_timestamp", "5 minutes")

    df_weather = df_weather_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), weather_schema).alias("data")).select("data.*") \
        .withWatermark("ingestion_timestamp", "5 minutes")

    df_camera = df_camera_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), camera_schema).alias("data")).select("data.*") \
        .withWatermark("ingestion_timestamp", "5 minutes")

    # --- JOIN DATA ---
    traffic_weather_joined = df_traffic.alias("t").join(
        df_weather.alias("w"),
        expr("""
            t.location_name = w.location_name AND
            w.ingestion_timestamp >= t.ingestion_timestamp - INTERVAL 3 MINUTES AND
            w.ingestion_timestamp <= t.ingestion_timestamp + INTERVAL 3 MINUTES
        """),
        "inner"
    )

    final_stream = traffic_weather_joined.join(
        df_camera.alias("c"),
        expr("""
            t.location_name = c.location_name AND
            c.ingestion_timestamp >= t.ingestion_timestamp - INTERVAL 3 MINUTES AND
            c.ingestion_timestamp <= t.ingestion_timestamp + INTERVAL 3 MINUTES
        """),
        "leftOuter"
    ).select(
        col("t.ingestion_timestamp").alias("time"),
        col("t.location_name"),
        col("w.weather_main").alias("weather_condition"),
        col("w.temperature"),
        col("t.tomtom_data.flowSegmentData.currentSpeed").alias("currentSpeed"), 
        col("t.tomtom_data.flowSegmentData.freeFlowSpeed").alias("freeFlowSpeed"),
        col("c.motorcycle_count"),
        col("c.car_count"),
        col("c.bus_truck_count")
    )

    # --- WRITE STREAM ---   
    query = final_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .option("checkpointLocation", "/opt/airflow/data/checkpoints/neon_v3") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()