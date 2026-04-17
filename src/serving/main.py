import os
import json
from contextlib import contextmanager
import datetime
from decimal import Decimal

import psycopg2
import psycopg2.extras
import redis
import psycopg2.errors
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(
    title="Traffic HCMC API - Distributed Architecture",
    description="API API cung cấp dữ liệu giao thông theo chuẩn Stream & CQRS",
    version="3.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Kích hoạt Prometheus Metrics Exporter
Instrumentator().instrument(app).expose(app)

# Khởi tạo Redis Client
# Trong file docker-compose.yml đã config REDIS_URL
redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379")
try:
    redis_client = redis.Redis.from_url(redis_url, decode_responses=True)
except Exception as e:
    print(f"Không thể kết nối đến Redis: {e}")
    redis_client = None

@contextmanager
def get_db():
    conn = psycopg2.connect(
        os.environ["POSTGRES_URL"],
        cursor_factory=psycopg2.extras.RealDictCursor
    )
    try:
        yield conn
    finally:
        conn.close()


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


with get_db() as _conn:
    ensure_prediction_columns(_conn)

# Custom JSON encoder to handle datetime parsing for json.dumps
def json_serial(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

@app.get("/api/health")
def health_check():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    COUNT(*) AS total_records,
                    MAX("time") AS latest_data
                FROM realtime_traffic_weather
                """
            )
            row = cur.fetchone() or {}

        return {
            "status": "ok",
            "message": "Hệ thống Serving API đã hoạt động tốt",
            "total_records": row.get("total_records", 0),
            "latest_data": row.get("latest_data"),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/diagnostics/camera-coverage")
def get_camera_coverage(hours: int = Query(default=1, ge=1, le=24)):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                WITH recent_traffic AS (
                    SELECT location_name, "time" AS event_time
                    FROM realtime_traffic_weather
                    WHERE "time" >= NOW() - (%s || ' hours')::INTERVAL
                ),
                paired AS (
                    SELECT
                        t.location_name,
                        t.event_time,
                        c."time" AS camera_time,
                        COALESCE(c.motorcycle_count, 0) AS motorcycle_count,
                        COALESCE(c.car_count, 0) AS car_count,
                        COALESCE(c.bus_truck_count, 0) AS bus_truck_count
                    FROM recent_traffic t
                    LEFT JOIN LATERAL (
                        SELECT
                            c."time",
                            c.motorcycle_count,
                            c.car_count,
                            c.bus_truck_count
                        FROM realtime_camera c
                        WHERE c.location_name = t.location_name
                          AND c."time" BETWEEN (t.event_time - INTERVAL '2 minutes')
                                            AND (t.event_time + INTERVAL '2 minutes')
                        ORDER BY ABS(EXTRACT(EPOCH FROM (c."time" - t.event_time))) ASC
                        LIMIT 1
                    ) c ON TRUE
                ),
                agg AS (
                    SELECT
                        location_name,
                        COUNT(*) AS traffic_rows,
                        COUNT(camera_time) AS matched_camera_rows,
                        SUM(
                            CASE
                                WHEN motorcycle_count = 0
                                 AND car_count = 0
                                 AND bus_truck_count = 0
                                THEN 1 ELSE 0
                            END
                        ) AS zero_vehicle_rows,
                        MAX(camera_time) AS latest_matched_camera_time
                    FROM paired
                    GROUP BY location_name
                ),
                latest_camera AS (
                    SELECT
                        location_name,
                        MAX("time") AS latest_camera_time
                    FROM realtime_camera
                    GROUP BY location_name
                )
                SELECT
                    a.location_name,
                    a.traffic_rows,
                    a.matched_camera_rows,
                    a.zero_vehicle_rows,
                    ROUND((a.matched_camera_rows::numeric / NULLIF(a.traffic_rows, 0)) * 100, 2) AS coverage_pct,
                    ROUND((a.zero_vehicle_rows::numeric / NULLIF(a.traffic_rows, 0)) * 100, 2) AS zero_pct,
                    lc.latest_camera_time,
                    EXTRACT(EPOCH FROM (NOW() - lc.latest_camera_time)) / 60.0 AS stale_minutes
                FROM agg a
                LEFT JOIN latest_camera lc
                  ON lc.location_name = a.location_name
                ORDER BY coverage_pct ASC NULLS LAST, zero_pct DESC NULLS LAST, a.location_name ASC
                """,
                (str(hours),),
            )
            rows = cur.fetchall() or []

        data = []
        for row in rows:
            item = dict(row)
            stale_minutes = item.get("stale_minutes")
            item["stale_minutes"] = round(float(stale_minutes), 2) if stale_minutes is not None else None
            data.append(item)

        critical = [
            r for r in data
            if (r.get("coverage_pct") is None or r.get("coverage_pct", 0) < 50)
            or (r.get("stale_minutes") is None or r.get("stale_minutes", 0) > 15)
        ]

        return {
            "status": "ok",
            "window_hours": hours,
            "location_count": len(data),
            "critical_count": len(critical),
            "critical_locations": [c.get("location_name") for c in critical],
            "data": data,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/traffic/latest")
def get_latest_traffic(limit: int = Query(default=50, le=200)):
    cache_key = f"traffic_latest_{limit}"
    
    # 1. Thực hiện Check Redis Cache Hit
    if redis_client:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            return json.loads(cached_data)

    # 2. Nếu Cache Miss, gọi vào DB
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT ON (location_name)
                    v.event_time, v.location_name,
                    v.latitude, v.longitude,
                    v.weather_condition, v.temperature, v.humidity, v.wind_speed, v.visibility,
                    v.current_speed, v.free_flow_speed, v.speed_ratio,
                    v.incident_count, v.congestion_level, v.congestion_label,
                    v.motorcycle_count, v.car_count, v.bus_truck_count,
                    v.camera_matched,
                    v.matched_camera_time,
                    (NOT v.camera_matched) AS no_camera_feed,
                    t.predicted_speed,
                    t.predicted_congestion_label,
                    COALESCE(t.predicted_speed_5m, t.predicted_speed) AS predicted_speed_5m,
                    t.predicted_speed_10m,
                    t.predicted_speed_15m,
                    COALESCE(t.predicted_congestion_label_5m, t.predicted_congestion_label) AS predicted_congestion_label_5m,
                    t.predicted_congestion_label_10m,
                    t.predicted_congestion_label_15m
                FROM traffic_view v
                LEFT JOIN realtime_traffic_weather t
                  ON t.location_name = v.location_name
                 AND t."time" = v.event_time
                ORDER BY location_name, event_time DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
            
        data_list = [dict(r) for r in rows]
        
        # 3. Ghi lại vào Cache với tuổi thọ 300s (5 phút)
        if redis_client:
            redis_client.setex(
                cache_key, 
                300, 
                json.dumps({"status": "ok", "count": len(data_list), "data": data_list, "source": "redis_cache"}, default=json_serial)
            )
            
        return {"status": "ok", "count": len(data_list), "data": data_list, "source": "postgresql"}
    except psycopg2.errors.UndefinedTable:
        return {"status": "ok", "count": 0, "data": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/traffic/summary")
def get_traffic_summary():
    cache_key = "traffic_summary"
    
    if redis_client:
        cached_data = redis_client.get(cache_key)
        if cached_data:
            return json.loads(cached_data)
        
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    location_name,
                    ROUND(AVG(current_speed)::numeric, 2)  AS avg_speed,
                    ROUND(AVG(speed_ratio)::numeric, 3)    AS avg_speed_ratio,
                    MODE() WITHIN GROUP (ORDER BY congestion_label) AS most_common_status,
                    SUM(incident_count)                    AS total_incidents,
                    SUM(motorcycle_count)                  AS total_motorcycle,
                    SUM(car_count)                         AS total_car,
                    SUM(bus_truck_count)                   AS total_bus_truck,
                    SUM(COALESCE(motorcycle_count,0) + COALESCE(car_count,0) + COALESCE(bus_truck_count,0))
                                                       AS total_vehicles,
                    COUNT(*)                               AS total_records
                FROM traffic_view
                WHERE event_time >= NOW() - INTERVAL '1 hour'
                GROUP BY location_name
                ORDER BY avg_speed ASC NULLS LAST
            """)
            rows = cur.fetchall()
            
        data_list = [dict(r) for r in rows]
        
        if redis_client:
            redis_client.setex(
                cache_key, 
                300, 
                json.dumps({"status": "ok", "period": "last_1_hour", "data": data_list, "source": "redis_cache"}, default=json_serial)
            )
            
        return {"status": "ok", "period": "last_1_hour", "data": data_list, "source": "postgresql"}
    except psycopg2.errors.UndefinedTable:
        return {"status": "ok", "count": 0, "data": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/weather/impact")
def get_weather_impact():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    weather_condition,
                    ROUND(AVG(current_speed)::numeric, 2) AS avg_speed,
                    ROUND(AVG(temperature)::numeric, 1)   AS avg_temperature,
                    ROUND(AVG(speed_ratio)::numeric, 3)   AS avg_speed_ratio,
                    COUNT(*)                               AS sample_count
                FROM traffic_view
                WHERE weather_condition IS NOT NULL
                GROUP BY weather_condition
                ORDER BY avg_speed ASC NULLS LAST
                """
            )
            rows = cur.fetchall()
        return {"status": "ok", "data": [dict(r) for r in rows]}
    except psycopg2.errors.UndefinedTable:
        return {"status": "ok", "count": 0, "data": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/traffic/location/{location_name}")
def get_location_history(
    location_name: str,
    limit: int = Query(default=20, le=100)
):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    v.event_time, v.current_speed, v.free_flow_speed,
                    v.speed_ratio, v.congestion_level, v.congestion_label,
                    v.weather_condition, v.temperature,
                    v.motorcycle_count, v.car_count, v.bus_truck_count,
                    v.camera_matched,
                    v.matched_camera_time,
                    (NOT v.camera_matched) AS no_camera_feed,
                    t.predicted_speed,
                    t.predicted_congestion_label,
                    COALESCE(t.predicted_speed_5m, t.predicted_speed) AS predicted_speed_5m,
                    t.predicted_speed_10m,
                    t.predicted_speed_15m,
                    COALESCE(t.predicted_congestion_label_5m, t.predicted_congestion_label) AS predicted_congestion_label_5m,
                    t.predicted_congestion_label_10m,
                    t.predicted_congestion_label_15m
                FROM traffic_view v
                LEFT JOIN realtime_traffic_weather t
                  ON t.location_name = v.location_name
                 AND t."time" = v.event_time
                WHERE v.location_name = %s
                                ORDER BY v.event_time DESC
                LIMIT %s
            """, (location_name, limit))
            rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail=f"Không tìm thấy địa điểm: {location_name}")
        return {"status": "ok", "location": location_name, "data": [dict(r) for r in rows]}
    except HTTPException:
        raise
    except psycopg2.errors.UndefinedTable:
        return {"status": "ok", "count": 0, "data": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/traffic/horizon/{location_name}")
def get_location_horizon(location_name: str):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT
                    v.event_time,
                    v.location_name,
                    v.current_speed,
                    v.free_flow_speed,
                    v.congestion_label,
                    v.camera_matched,
                    v.matched_camera_time,
                    (NOT v.camera_matched) AS no_camera_feed,
                    t.predicted_speed,
                    t.predicted_congestion_label,
                    COALESCE(t.predicted_speed_5m, t.predicted_speed) AS predicted_speed_5m,
                    t.predicted_speed_10m,
                    t.predicted_speed_15m,
                    COALESCE(t.predicted_congestion_label_5m, t.predicted_congestion_label) AS predicted_congestion_label_5m,
                    t.predicted_congestion_label_10m,
                    t.predicted_congestion_label_15m,
                    t.model_version
                FROM traffic_view v
                LEFT JOIN realtime_traffic_weather t
                  ON t.location_name = v.location_name
                 AND t."time" = v.event_time
                WHERE v.location_name = %s
                ORDER BY v.event_time DESC
                LIMIT 1
                """,
                (location_name,),
            )
            row = cur.fetchone()

        if not row:
            raise HTTPException(status_code=404, detail=f"Không tìm thấy địa điểm: {location_name}")

        payload = dict(row)
        payload["horizons"] = {
            "5m": {
                "speed": payload.get("predicted_speed_5m"),
                "label": payload.get("predicted_congestion_label_5m"),
            },
            "10m": {
                "speed": payload.get("predicted_speed_10m"),
                "label": payload.get("predicted_congestion_label_10m"),
            },
            "15m": {
                "speed": payload.get("predicted_speed_15m"),
                "label": payload.get("predicted_congestion_label_15m"),
            },
        }
        return {"status": "ok", "data": payload}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/traffic/chart/{location_name}")
def get_location_chart(
    location_name: str,
    hours: int = Query(default=3, ge=1, le=24)
):
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    date_trunc('minute', event_time) -
                        (EXTRACT(minute FROM event_time)::int % 5) * INTERVAL '1 minute' AS bucket,
                    ROUND(AVG(current_speed)::numeric, 1) AS avg_speed,
                    ROUND(AVG(speed_ratio)::numeric, 3)   AS avg_ratio
                FROM traffic_view
                WHERE location_name = %s
                  AND event_time >= NOW() - (%s || ' hours')::INTERVAL
                GROUP BY bucket
                ORDER BY bucket ASC
            """, (location_name, str(hours)))
            rows = cur.fetchall()
        return {"status": "ok", "location": location_name, "period_hours": hours, "data": [dict(r) for r in rows]}
    except psycopg2.errors.UndefinedTable:
        return {"status": "ok", "count": 0, "data": []}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
