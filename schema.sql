-- ============================================================
-- SCHEMA khớp với spark_processor.py CQRS Event-driven (V2 Decoupled)
-- Chạy trên PostgreSQL local self-host
-- ============================================================

CREATE TABLE IF NOT EXISTS realtime_traffic_weather (
    "time"            TIMESTAMPTZ      NOT NULL,
    location_name     TEXT             NOT NULL,
    weather_condition TEXT,
    temperature       DOUBLE PRECISION,
    humidity          DOUBLE PRECISION,
    wind_speed        DOUBLE PRECISION,
    visibility        DOUBLE PRECISION,
    "currentSpeed"    DOUBLE PRECISION,
    "freeFlowSpeed"   DOUBLE PRECISION,
    incident_count    INTEGER,
    predicted_speed             DOUBLE PRECISION,
    predicted_congestion_label  TEXT,
    predicted_speed_5m          DOUBLE PRECISION,
    predicted_speed_10m         DOUBLE PRECISION,
    predicted_speed_15m         DOUBLE PRECISION,
    predicted_congestion_label_5m  TEXT,
    predicted_congestion_label_10m TEXT,
    predicted_congestion_label_15m TEXT,
    model_version               TEXT,
    PRIMARY KEY (location_name, "time")
);

CREATE INDEX IF NOT EXISTS idx_traffic_time
    ON realtime_traffic_weather ("time" DESC);
CREATE INDEX IF NOT EXISTS idx_traffic_loc_time
    ON realtime_traffic_weather (location_name, "time" DESC);

-- Bảng lưu trữ độc lập cho Camera
CREATE TABLE IF NOT EXISTS realtime_camera (
    "time"            TIMESTAMPTZ      NOT NULL,
    location_name     TEXT             NOT NULL,
    motorcycle_count  DOUBLE PRECISION,
    car_count         DOUBLE PRECISION,
    bus_truck_count   DOUBLE PRECISION,
    PRIMARY KEY (location_name, "time")
);

CREATE INDEX IF NOT EXISTS idx_camera_loc_time
    ON realtime_camera (location_name, "time" DESC);

-- Bảng theo dõi chất lượng pipeline theo batch (invalid, DLQ, ghi thành công)
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

-- ============================================================
-- VIEW sạch -- FastAPI chỉ đọc từ đây, đã ghép Camera và Traffic bằng LEFT JOIN
-- ============================================================
CREATE OR REPLACE VIEW traffic_view AS
SELECT
    t."time"                                            AS event_time,
    t.location_name,
    t.weather_condition,
    t.temperature,
    t.humidity,
    t.wind_speed,
    t.visibility,
    t."currentSpeed"                                    AS current_speed,
    t."freeFlowSpeed"                                   AS free_flow_speed,
    t.incident_count,
    t.predicted_speed,
    t.predicted_congestion_label,
    t.predicted_speed_5m,
    t.predicted_speed_10m,
    t.predicted_speed_15m,
    t.predicted_congestion_label_5m,
    t.predicted_congestion_label_10m,
    t.predicted_congestion_label_15m,
    t.model_version,

    CASE
        WHEN t."freeFlowSpeed" > 0
        THEN ROUND((t."currentSpeed" / t."freeFlowSpeed")::numeric, 3)
        ELSE NULL
    END                                                 AS speed_ratio,

    CASE
        WHEN t."currentSpeed" IS NULL OR t."freeFlowSpeed" IS NULL OR t."freeFlowSpeed" = 0 THEN NULL
        WHEN t."currentSpeed" / t."freeFlowSpeed" >= 0.8  THEN 1
        WHEN t."currentSpeed" / t."freeFlowSpeed" >= 0.5  THEN 2
        WHEN t."currentSpeed" / t."freeFlowSpeed" >= 0.25 THEN 3
        ELSE 4
    END                                                 AS congestion_level,

    CASE
        WHEN t."currentSpeed" IS NULL OR t."freeFlowSpeed" IS NULL OR t."freeFlowSpeed" = 0 THEN 'Không rõ'
        WHEN t."currentSpeed" / t."freeFlowSpeed" >= 0.8  THEN 'Thông thoáng'
        WHEN t."currentSpeed" / t."freeFlowSpeed" >= 0.5  THEN 'Bình thường'
        WHEN t."currentSpeed" / t."freeFlowSpeed" >= 0.25 THEN 'Chậm'
        ELSE 'Tắc nghẽn'
    END                                                 AS congestion_label,

    CASE t.location_name
        WHEN 'Nga_Tu_Hang_Xanh'      THEN 10.8015
        WHEN 'Vong_Xoay_Lang_Cha_Ca' THEN 10.8023
        WHEN 'Cau_Kenh_Te'           THEN 10.7523
        WHEN 'Nga_Tu_Thu_Duc'        THEN 10.8504
        WHEN 'Nut_Giao_Ly_Thuong_Kiet_3Thang2' THEN 10.763875
        WHEN 'Nut_Giao_Bac_Hai_Thanh_Thai' THEN 10.78082
        WHEN 'Nga_Tu_Thanh_Thai_To_Hien_Thanh' THEN 10.77639
        WHEN 'Nut_Giao_3Thang2_Thanh_Thai' THEN 10.76778
        WHEN 'Nut_Giao_Su_Van_Hanh_To_Hien_Thanh' THEN 10.77795
        WHEN 'Nut_Giao_3Thang2_Su_Van_Hanh' THEN 10.76962
        WHEN 'Nut_Giao_CMT8_Dien_Bien_Phu' THEN 10.77676
        WHEN 'Nut_Giao_Cao_Thang_Dien_Bien_Phu' THEN 10.77276
        WHEN 'Nut_Giao_3Thang2_Le_Hong_Phong' THEN 10.77091
        WHEN 'Nut_Giao_3Thang2_Cao_Thang' THEN 10.77381
        WHEN 'Nut_Giao_3Thang2_Ly_Thai_To' THEN 10.7681
        WHEN 'Bung_binh_Ly_Thai_To' THEN 10.76776
        WHEN 'Nut_Giao_Truong_Chinh_Au_Co' THEN 10.80184
        WHEN 'Nut_Giao_3Thang2_Le_Dai_Hanh' THEN 10.76226
        WHEN 'Nut_Giao_Lac_Long_Quan_Ly_Thuong_Kiet' THEN 10.79034
        WHEN 'Nut_Giao_Lac_Long_Quan_Au_Co' THEN 10.77478
        WHEN 'Nut_Giao_Lu_Gia_Nguyen_Thi_Nho' THEN 10.77115
        WHEN 'Nut_Giao_Au_Co_Nguyen_Thi_Nho' THEN 10.76906
        WHEN 'Duong_Au_Co' THEN 10.78938
        WHEN 'Nut_Giao_Cong_Hoa_Hoang_Hoa_Tham' THEN 10.80184
        WHEN 'Nut_Giao_Truong_Chinh_Hoang_Hoa_Tham' THEN 10.79637
        WHEN 'Nut_Giao_Truong_Chinh_Cong_Hoa' THEN 10.80751
    END                                                 AS latitude,

    CASE t.location_name
        WHEN 'Nga_Tu_Hang_Xanh'      THEN 106.7111
        WHEN 'Vong_Xoay_Lang_Cha_Ca' THEN 106.6603
        WHEN 'Cau_Kenh_Te'           THEN 106.6972
        WHEN 'Nga_Tu_Thu_Duc'        THEN 106.7716
        WHEN 'Nut_Giao_Ly_Thuong_Kiet_3Thang2' THEN 106.660007
        WHEN 'Nut_Giao_Bac_Hai_Thanh_Thai' THEN 106.65883
        WHEN 'Nga_Tu_Thanh_Thai_To_Hien_Thanh' THEN 106.66353
        WHEN 'Nut_Giao_3Thang2_Thanh_Thai' THEN 106.66702
        WHEN 'Nut_Giao_Su_Van_Hanh_To_Hien_Thanh' THEN 106.66557
        WHEN 'Nut_Giao_3Thang2_Su_Van_Hanh' THEN 106.67076
        WHEN 'Nut_Giao_CMT8_Dien_Bien_Phu' THEN 106.68371
        WHEN 'Nut_Giao_Cao_Thang_Dien_Bien_Phu' THEN 106.679
        WHEN 'Nut_Giao_3Thang2_Le_Hong_Phong' THEN 106.67311
        WHEN 'Nut_Giao_3Thang2_Cao_Thang' THEN 106.67778
        WHEN 'Nut_Giao_3Thang2_Ly_Thai_To' THEN 106.66803
        WHEN 'Bung_binh_Ly_Thai_To' THEN 106.67418
        WHEN 'Nut_Giao_Truong_Chinh_Au_Co' THEN 106.63674
        WHEN 'Nut_Giao_3Thang2_Le_Dai_Hanh' THEN 106.65707
        WHEN 'Nut_Giao_Lac_Long_Quan_Ly_Thuong_Kiet' THEN 106.65236
        WHEN 'Nut_Giao_Lac_Long_Quan_Au_Co' THEN 106.64798
        WHEN 'Nut_Giao_Lu_Gia_Nguyen_Thi_Nho' THEN 106.65292
        WHEN 'Nut_Giao_Au_Co_Nguyen_Thi_Nho' THEN 106.65226
        WHEN 'Duong_Au_Co' THEN 106.6404
        WHEN 'Nut_Giao_Cong_Hoa_Hoang_Hoa_Tham' THEN 106.64742
        WHEN 'Nut_Giao_Truong_Chinh_Hoang_Hoa_Tham' THEN 106.64694
        WHEN 'Nut_Giao_Truong_Chinh_Cong_Hoa' THEN 106.63481
    END                                                 AS longitude,

    COALESCE(c.motorcycle_count, 0)::int  AS motorcycle_count,
    COALESCE(c.car_count, 0)::int         AS car_count,
    COALESCE(c.bus_truck_count, 0)::int   AS bus_truck_count,
    (c.camera_time IS NOT NULL)           AS camera_matched,
    c.camera_time                         AS matched_camera_time
FROM realtime_traffic_weather t
LEFT JOIN LATERAL (
    SELECT
        c."time" AS camera_time,
        c.motorcycle_count,
        c.car_count,
        c.bus_truck_count
    FROM realtime_camera c
    WHERE c.location_name = t.location_name
      AND c."time" BETWEEN (t."time" - INTERVAL '2 minutes')
                        AND (t."time" + INTERVAL '2 minutes')
    ORDER BY ABS(EXTRACT(EPOCH FROM (c."time" - t."time"))) ASC
    LIMIT 1
) c ON TRUE;