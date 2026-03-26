-- ============================================================
-- SCHEMA khớp hoàn toàn với spark_processor.py gốc
-- Chạy trên Neon PostgreSQL TRƯỚC khi khởi động hệ thống
-- ============================================================

CREATE TABLE IF NOT EXISTS realtime_traffic_weather (
    "time"            TIMESTAMPTZ      NOT NULL,
    location_name     TEXT             NOT NULL,
    weather_condition TEXT,
    temperature       DOUBLE PRECISION,
    "currentSpeed"    DOUBLE PRECISION,
    "freeFlowSpeed"   DOUBLE PRECISION,
    motorcycle_count  DOUBLE PRECISION,
    car_count         DOUBLE PRECISION,
    bus_truck_count   DOUBLE PRECISION
);

CREATE INDEX IF NOT EXISTS idx_traffic_time
    ON realtime_traffic_weather ("time" DESC);
CREATE INDEX IF NOT EXISTS idx_traffic_loc_time
    ON realtime_traffic_weather (location_name, "time" DESC);

-- ============================================================
-- VIEW sạch -- FastAPI chỉ đọc từ đây, không đụng bảng gốc
-- ============================================================
CREATE OR REPLACE VIEW traffic_view AS
SELECT
    "time"                                              AS event_time,
    location_name,
    weather_condition,
    temperature,
    "currentSpeed"                                      AS current_speed,
    "freeFlowSpeed"                                     AS free_flow_speed,

    CASE
        WHEN "freeFlowSpeed" > 0
        THEN ROUND(("currentSpeed" / "freeFlowSpeed")::numeric, 3)
        ELSE NULL
    END                                                 AS speed_ratio,

    CASE
        WHEN "currentSpeed" IS NULL OR "freeFlowSpeed" IS NULL OR "freeFlowSpeed" = 0 THEN NULL
        WHEN "currentSpeed" / "freeFlowSpeed" >= 0.8  THEN 1
        WHEN "currentSpeed" / "freeFlowSpeed" >= 0.5  THEN 2
        WHEN "currentSpeed" / "freeFlowSpeed" >= 0.25 THEN 3
        ELSE 4
    END                                                 AS congestion_level,

    CASE
        WHEN "currentSpeed" IS NULL OR "freeFlowSpeed" IS NULL OR "freeFlowSpeed" = 0 THEN 'Không rõ'
        WHEN "currentSpeed" / "freeFlowSpeed" >= 0.8  THEN 'Thông thoáng'
        WHEN "currentSpeed" / "freeFlowSpeed" >= 0.5  THEN 'Bình thường'
        WHEN "currentSpeed" / "freeFlowSpeed" >= 0.25 THEN 'Chậm'
        ELSE 'Tắc nghẽn'
    END                                                 AS congestion_label,

    CASE location_name
        WHEN 'Nga_Tu_Hang_Xanh'      THEN 10.8015
        WHEN 'Vong_Xoay_Lang_Cha_Ca' THEN 10.8023
        WHEN 'Cau_Kenh_Te'           THEN 10.7523
        WHEN 'Nga_Tu_Thu_Duc'        THEN 10.8504
    END                                                 AS latitude,

    CASE location_name
        WHEN 'Nga_Tu_Hang_Xanh'      THEN 106.7111
        WHEN 'Vong_Xoay_Lang_Cha_Ca' THEN 106.6603
        WHEN 'Cau_Kenh_Te'           THEN 106.6972
        WHEN 'Nga_Tu_Thu_Duc'        THEN 106.7716
    END                                                 AS longitude,

    motorcycle_count::int  AS motorcycle_count,
    car_count::int         AS car_count,
    bus_truck_count::int   AS bus_truck_count
FROM realtime_traffic_weather;