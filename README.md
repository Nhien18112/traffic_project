# HCMC Traffic Intelligence

Real-time traffic monitoring and analytics for Ho Chi Minh City using Kafka, Spark Streaming, YOLOv8, FastAPI, PostgreSQL, and a redesigned Web UI.

## Overview

The system ingests three live sources:
1. Camera snapshots from HCMC traffic cameras
2. TomTom traffic flow data
3. OpenWeather weather data

Pipeline behavior:
1. Ingestion service publishes camera/weather/traffic events to Kafka.
2. AI worker consumes camera.raw, runs YOLOv8, publishes vehicle counts to camera.processed.
3. Spark streaming writes traffic-weather records to realtime_traffic_weather and camera counts to realtime_camera.
4. A serving view traffic_view correlates each traffic record with the nearest camera record in a +/-2 minute window.
5. FastAPI serves data to the web dashboard and diagnostics endpoints.

Note: the chat assistant/Groq integration has been removed from this project.

## Architecture

```text
Ingestion (async polling) -> Kafka topics -> Spark + AI worker -> PostgreSQL -> FastAPI -> Web UI

camera.raw ---------> ai_worker (YOLO) -> camera.processed ----+
traffic.raw -----------------------------------------------+   |
weather.raw -------------------------------------------+    |   |
                                                      Spark joins/writes
                                                           |
                                     realtime_traffic_weather + realtime_camera
                                                           |
                                                   traffic_view (time-aware join)
                                                           |
                                              FastAPI + camera diagnostics API
                                                           |
                                                     Vite/React dashboard
```

## Key Features

1. Time-aware camera matching in the serving view (avoids frozen vehicle counts).
2. Camera coverage diagnostics endpoint to detect missing/stale camera feeds.
3. Dashboard with hero layout, filters, insights, what-if simulator, and coverage panel.
4. No-camera-feed flags in API responses so UI can distinguish missing feeds from real zero counts.

## Quick Start

1. Create/update .env:

```env
TOMTOM_API_KEY=your_tomtom_api_key
WEATHER_API_KEY=your_openweather_api_key
POSTGRES_URL=postgresql://traffic_user:traffic_pass@postgres:5432/trafficdb
POSTGRES_JDBC_URL=jdbc:postgresql://postgres:5432/trafficdb
POSTGRES_USER=traffic_user
POSTGRES_PASSWORD=traffic_pass
```

2. Start services:

```bash
docker compose up -d --build
```

3. Check status:

```bash
docker compose ps
```

4. Open services:
1. Dashboard: http://localhost:3000
2. FastAPI docs: http://localhost:8000/docs
3. Kafka UI: http://localhost:8081
4. MinIO Console: http://localhost:9001
5. Prometheus: http://localhost:9090
6. Grafana: http://localhost:3001

## Core API Endpoints

1. GET /api/health
2. GET /api/traffic/latest
3. GET /api/traffic/summary
4. GET /api/weather/impact
5. GET /api/traffic/location/{location_name}
6. GET /api/traffic/horizon/{location_name}
7. GET /api/traffic/chart/{location_name}
8. GET /api/diagnostics/camera-coverage?hours=1

## Troubleshooting

### Vehicle counts look duplicated across timeline

Check that schema.sql has been re-applied after view updates:

```bash
docker compose exec -T postgres psql -U traffic_user -d trafficdb -c "DROP VIEW IF EXISTS traffic_view;"
docker compose exec -T postgres psql -U traffic_user -d trafficdb -f /docker-entrypoint-initdb.d/init.sql
```

### Many rows show zero vehicles

Use diagnostics API:

```bash
curl "http://localhost:8000/api/diagnostics/camera-coverage?hours=1"
```

If coverage is low for locations, check camera config in src/ingestion/config.py and polling logs:

```bash
docker compose logs --tail 120 polling-services
```

### Serving data looks stale

Clear Redis cache keys:

```bash
docker compose exec -T redis redis-cli DEL traffic_latest_50 traffic_summary
```

## Project Structure

```text
traffic_project/
├─ src/
│  ├─ ingestion/          # async polling (camera/weather/tomtom)
│  ├─ ai_worker/          # YOLO inference for vehicle counts
│  ├─ streaming/          # Spark + prediction sink
│  ├─ serving/            # FastAPI serving APIs
├─ web-ui/                # Vite React dashboard
├─ schema.sql             # DB schema + traffic_view definition
├─ docker-compose.yml
├─ Dockerfile.*
└─ .env
```

## License

MIT