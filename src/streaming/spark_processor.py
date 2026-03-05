from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

def main():
    spark = SparkSession.builder \
        .appName("TrafficWeatherStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # ==========================================
    # 1. ĐỊNH NGHĨA CẤU TRÚC DỮ LIỆU (SCHEMA)
    # ==========================================
    # Sửa lại tên cột dưới đây cho khớp với JSON bạn đã tạo ở fetch_tomtom.py
    traffic_schema = StructType([
        StructField("location_name", StringType(), True),
        StructField("currentSpeed", DoubleType(), True),
        StructField("jamFactor", DoubleType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])

    # Sửa lại tên cột dưới đây cho khớp với JSON bạn đã tạo ở fetch_weather.py
    weather_schema = StructType([
        StructField("location_name", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("ingestion_timestamp", TimestampType(), True)
    ])

    # ==========================================
    # 2. HÚT DỮ LIỆU TỪ 2 TOPIC KAFKA
    # ==========================================
    df_traffic_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_tomtom_traffic") \
        .option("startingOffsets", "latest").load()

    df_weather_raw = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "raw_weather_data") \
        .option("startingOffsets", "latest").load()

    # ==========================================
    # 3. GIẢI NÉN JSON VÀ ĐÓNG MỐC THỜI GIAN (WATERMARK)
    # ==========================================
    # Giải nén TomTom và cho phép trễ tối đa 5 phút
    df_traffic = df_traffic_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), traffic_schema).alias("data")).select("data.*") \
        .withWatermark("ingestion_timestamp", "5 minutes")

    # Giải nén Weather và cho phép trễ tối đa 5 phút
    df_weather = df_weather_raw.selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), weather_schema).alias("data")).select("data.*") \
        .withWatermark("ingestion_timestamp", "5 minutes")

    # ==========================================
    # 4. GHÉP NỐI (JOIN) 2 LUỒNG DỮ LIỆU
    # ==========================================
    # Điều kiện ghép: Cùng tên đường VÀ thời gian lấy dữ liệu cách nhau không quá 3 phút
    joined_stream = df_traffic.alias("t").join(
        df_weather.alias("w"),
        expr("""
            t.location_name = w.location_name AND
            w.ingestion_timestamp >= t.ingestion_timestamp - INTERVAL 3 MINUTES AND
            w.ingestion_timestamp <= t.ingestion_timestamp + INTERVAL 3 MINUTES
        """),
        "inner"
    ).select(
        col("t.ingestion_timestamp").alias("time"),
        col("t.location_name"),
        col("w.weather_condition"),
        col("w.temperature"),
        col("t.currentSpeed"),
        col("t.jamFactor")
    )

    # ==========================================
    # 5. XUẤT KẾT QUẢ RA MÀN HÌNH
    # ==========================================
    print("Đã kết nối Spark thành công! Đang chờ dữ liệu chảy qua...")
    query = joined_stream.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()