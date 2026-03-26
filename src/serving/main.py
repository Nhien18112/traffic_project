import os
from contextlib import contextmanager

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from models import TrafficRecord, LocationSummary, WeatherImpact

app = FastAPI(
    title="Traffic HCMC API",
    description="API cung cấp dữ liệu giao thông thời gian thực TPHCM",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@contextmanager
def get_db():
    """Context manager: tự động đóng connection sau khi dùng xong."""
    conn = psycopg2.connect(
        os.environ["NEON_POSTGRES_URL"],
        cursor_factory=psycopg2.extras.RealDictCursor
    )
    try:
        yield conn
    finally:
        conn.close()


@app.get("/api/health")
def health_check():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) AS total_records, MAX("time") AS latest_data
                FROM realtime_traffic_weather
            """)
            row = dict(cur.fetchone())
        return {
            "status":        "ok",
            "total_records": row["total_records"],
            "latest_data":   row["latest_data"],
            "message":       "Hệ thống đang hoạt động bình thường"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/traffic/latest")
def get_latest_traffic(limit: int = Query(default=50, le=200)):
    """Bản ghi mới nhất của mỗi địa điểm — dùng DISTINCT ON thay vì lọc ở frontend."""
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT DISTINCT ON (location_name)
                    event_time, location_name,
                    latitude, longitude,
                    weather_condition, temperature,
                    current_speed, free_flow_speed, speed_ratio,
                    congestion_level, congestion_label,
                    motorcycle_count, car_count, bus_truck_count
                FROM traffic_view
                ORDER BY location_name, event_time DESC
                LIMIT %s
            """, (limit,))
            rows = cur.fetchall()
        return {"status": "ok", "count": len(rows), "data": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/traffic/summary")
def get_traffic_summary():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    location_name,
                    ROUND(AVG(current_speed)::numeric, 2)  AS avg_speed,
                    ROUND(AVG(speed_ratio)::numeric, 3)    AS avg_speed_ratio,
                    MODE() WITHIN GROUP (ORDER BY congestion_label) AS most_common_status,
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
        return {"status": "ok", "period": "last_1_hour", "data": [dict(r) for r in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/weather/impact")
def get_weather_impact():
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    weather_condition,
                    ROUND(AVG(current_speed)::numeric, 2) AS avg_speed,
                    ROUND(AVG(temperature)::numeric, 1)   AS avg_temperature,
                    ROUND(AVG(speed_ratio)::numeric, 3)   AS avg_speed_ratio,
                    COUNT(*) AS sample_count
                FROM traffic_view
                WHERE weather_condition IS NOT NULL
                GROUP BY weather_condition
                ORDER BY avg_speed ASC NULLS LAST
            """)
            rows = cur.fetchall()
        return {"status": "ok", "data": [dict(r) for r in rows]}
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
                    event_time, current_speed, free_flow_speed,
                    speed_ratio, congestion_level, congestion_label,
                    weather_condition, temperature,
                    motorcycle_count, car_count, bus_truck_count
                FROM traffic_view
                WHERE location_name = %s
                ORDER BY event_time DESC
                LIMIT %s
            """, (location_name, limit))
            rows = cur.fetchall()
        if not rows:
            raise HTTPException(status_code=404, detail=f"Không tìm thấy địa điểm: {location_name}")
        return {"status": "ok", "location": location_name, "data": [dict(r) for r in rows]}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/traffic/chart/{location_name}")
def get_location_chart(
    location_name: str,
    hours: int = Query(default=3, ge=1, le=24)
):
    """Dữ liệu vẽ biểu đồ tốc độ — nhóm theo bucket 5 phút."""
    try:
        with get_db() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    date_trunc('minute', event_time) -
                        (EXTRACT(minute FROM event_time)::int %% 5) * INTERVAL '1 minute' AS bucket,
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
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))