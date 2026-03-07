from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Hàm này sẽ được gọi mỗi khi Spark gom đủ 1 mẻ dữ liệu mới (Micro-batch)
def process_batch(df, epoch_id):
    df.show(truncate=False)
    
    # 2. Đẩy thẳng mẻ dữ liệu này vào PostgreSQL
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/airflow") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "realtime_traffic_weather") \
        .option("user", "airflow") \
        .option("password", "airflow") \
        .mode("append") \
        .save()

def main():
    # Thêm thư viện Postgres JDBC để Spark biết cách nói chuyện với Database
    spark = SparkSession.builder \
        .appName("TrafficWeatherStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. ĐỊNH NGHĨA LẠI SCHEMA    
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

    # Thêm Schema cho dữ liệu Camera AI
    camera_schema = StructType([
        StructField("location_name", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True),
        StructField("motorcycle_count", DoubleType(), True),
        StructField("car_count", DoubleType(), True),
        StructField("bus_truck_count", DoubleType(), True)
    ])

    # 2. HÚT DỮ LIỆU TỪ KAFKA VÀ WATERMARK

    df_traffic_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_tomtom_traffic") \
        .option("startingOffsets", "latest").load()

    df_weather_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_weather_data") \
        .option("startingOffsets", "latest").load()

    df_camera_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_camera_traffic") \
        .option("startingOffsets", "latest").load()

    df_traffic = df_traffic_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), traffic_schema).alias("data")).select("data.*") \
        .withWatermark("ingestion_timestamp", "5 minutes")

    df_weather = df_weather_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), weather_schema).alias("data")).select("data.*") \
        .withWatermark("ingestion_timestamp", "5 minutes")

    df_camera = df_camera_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), camera_schema).alias("data")).select("data.*") \
        .withWatermark("ingestion_timestamp", "5 minutes")

    # 3. GHÉP NỐI (JOIN) 3 LUỒNG DỮ LIỆU

    # Nối Giao thông và Thời tiết
    traffic_weather_joined = df_traffic.alias("t").join(
        df_weather.alias("w"),
        expr("""
            t.location_name = w.location_name AND
            w.ingestion_timestamp >= t.ingestion_timestamp - INTERVAL 3 MINUTES AND
            w.ingestion_timestamp <= t.ingestion_timestamp + INTERVAL 3 MINUTES
        """),
        "inner"
    )

    #  Nối tiếp kết quả với Camera 
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

    # 4. XUẤT KẾT QUẢ VÀO POSTGRES 

    query = final_stream.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()