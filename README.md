# 🚦 HCMC Traffic Intelligence

> A real-time traffic monitoring and analysis system for Ho Chi Minh City, Vietnam — powered by Apache Kafka, Spark Streaming, YOLOv8, and a modern web dashboard.

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=flat-square&logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5.0-black?style=flat-square&logo=apachekafka)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.0-orange?style=flat-square&logo=apachespark)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111.0-009688?style=flat-square&logo=fastapi)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=flat-square&logo=docker)

---

## 📌 Overview

**HCMC Traffic Intelligence** is a big data pipeline that collects, processes, and visualizes real-time traffic data across key intersections in Ho Chi Minh City.

The system integrates three data sources:
- **Traffic cameras** from the HCMC Department of Transportation (live MJPEG streams)
- **TomTom Traffic API** for real-time speed and flow data
- **OpenWeatherMap API** for weather conditions

Vehicle counts are extracted from camera images using a **YOLOv8** object detection model running as a dedicated AI worker. All streams are joined and processed via **Apache Spark Streaming** and stored in a **Neon PostgreSQL** cloud database, served through a **FastAPI** backend and a clean web dashboard.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     INGESTION LAYER                      │
│  Airflow DAG (every 3 min)                               │
│  ├── fetch_hcm_traffic.py  → Camera images (4 locations) │
│  ├── fetch_tomtom.py       → Speed / flow data           │
│  └── fetch_weather.py      → Temperature / conditions    │
└───────────────────────┬─────────────────────────────────┘
                        │
              ┌─────────▼─────────┐
              │   Apache Kafka    │
              │  4 topics         │
              └─────────┬─────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
   ┌────▼────┐    ┌──────▼──────┐  ┌───▼────┐
   │  Spark  │    │  AI Worker  │  │ MinIO  │
   │Streaming│    │  YOLOv8     │  │  Raw   │
   │  Join   │    │  vehicle    │  │  Data  │
   │3 streams│    │  counting   │  │  Lake  │
   └────┬────┘    └─────────────┘  └────────┘
        │
┌───────▼──────────┐
│  Neon PostgreSQL │
│  (ap-southeast-1)│
└───────┬──────────┘
        │
┌───────▼──────────┐
│  FastAPI Serving │  ← REST API
│  + Web Dashboard │  ← Nginx static
└──────────────────┘
```

---

## 📍 Monitored Locations

Map: https://www.google.com/maps/d/u/0/edit?mid=1Ic6AgHtYSf2DEyzaeFdfbt9qS31hmwE&usp=sharing

| Location | Coordinates |
|---|---|
| Ngã Tư Hàng Xanh | 10.8015, 106.7111 |
| Vòng Xoay Lăng Cha Cả | 10.8023, 106.6603 |
| Cầu Kênh Tẻ | 10.7523, 106.6972 |
| Ngã Tư Thủ Đức | 10.8504, 106.7716 |
| Nút Giao Lý Thường Kiệt - 3 Tháng 2 | 10.763875, 106.660007 |
| Nút Giao Bắc Hải - Thành Thái | 10.78082, 106.65883 |
| Ngã Tư Thành Thái - Tô Hiến Thành | 10.77639, 106.66353 |
| Nút Giao 3 Tháng 2 - Thành Thái | 10.76778, 106.66702 |
| Nút Giao Sư Vạn Hạnh - Tô Hiến Thành | 10.77795, 106.66557 |
| Nút Giao 3 Tháng 2 - Sư Vạn Hạnh | 10.76962, 106.67076 |
| Nút Giao CMT8 - Điện Biên Phủ | 10.77676, 106.68371 |
| Nút Giao Cao Thắng - Điện Biên Phủ | 10.77276, 106.679 |
| Nút Giao 3 Tháng 2 - Lê Hồng Phong | 10.77091, 106.67311 |
| Nút Giao 3 Tháng 2 - Cao Thắng | 10.77381, 106.67778 |
| Nút Giao 3 Tháng 2 - Lý Thái Tổ | 10.7681, 106.66803 |
| Bùng binh Lý Thái Tổ | 10.76776, 106.67418 |
| Nút Giao Trường Chinh - Âu Cơ | 10.80184, 106.63674 |
| Nút Giao 3 Tháng 2 - Lê Đại Hành | 10.76226, 106.65707 |
| Nút Giao Lạc Long Quân - Lý Thường Kiệt | 10.79034, 106.65236 |
| Nút Giao Lạc Long Quân - Âu Cơ | 10.77478, 106.64798 |
| Nút Giao Lữ Gia - Nguyễn Thị Nhỏ | 10.77115, 106.65292 |
| Nút Giao Âu Cơ - Nguyễn Thị Nhỏ | 10.76906, 106.65226 |
| Đường Âu Cơ | 10.78938, 106.6404 |
| Nút Giao Cộng Hòa - Hoàng Hoa Thám | 10.80184, 106.64742 |
| Nút Giao Trường Chinh - Hoàng Hoa Thám | 10.79637, 106.64694 |

---

## 🧰 Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8.1 |
| Message Broker | Apache Kafka 7.5.0 |
| Stream Processing | Apache Spark 3.5.0 |
| AI / Computer Vision | YOLOv8 (Ultralytics) |
| Object Storage | MinIO (S3-compatible) |
| Cloud Database | Neon PostgreSQL |
| Backend API | FastAPI 0.111.0 |
| Web Server | Nginx (Alpine) |
| Containerization | Docker Compose |

---

## 🚀 Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- API keys for [TomTom](https://developer.tomtom.com/) and [OpenWeatherMap](https://openweathermap.org/api)
- A [Neon](https://neon.tech) PostgreSQL database

### 1. Clone the repository

```bash
git clone https://github.com/thinhtranthhung/project_traffic_intelligent.git
cd project_traffic_intelligent
```

### 2. Configure environment variables

```bash
cp .env.example .env
```

Edit `.env` and fill in your credentials:

```env
TOMTOM_API_KEY=your_tomtom_api_key
WEATHER_API_KEY=your_openweather_api_key
NEON_POSTGRES_URL=postgresql://...
NEON_JDBC_URL=jdbc:postgresql://...
NEON_USER=your_neon_user
NEON_PASSWORD=your_neon_password
```

### 3. Initialize the database

Run the SQL schema on your Neon database (one time only):

```bash
# Copy the contents of schema.sql and run it in the Neon SQL Editor
# at https://console.neon.tech
```

This creates the `realtime_traffic_weather` table and `traffic_view` with auto-computed congestion levels.

### 4. Build and start all services

```bash
docker compose up -d --build
```

First build takes ~3–5 minutes. Once complete:

```bash
docker compose ps   # all services should show "Up"
```

### 5. Enable the Airflow DAG

Go to [http://localhost:8082](http://localhost:8082) → login `admin / admin` → find `traffic_data_ingestion` → **toggle ON**.

Data will start flowing within 3 minutes.

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---|---|---|
| **Web Dashboard** | http://localhost:3000 | — |
| **FastAPI Swagger** | http://localhost:8000/docs | — |
| **Airflow UI** | http://localhost:8082 | admin / admin |
| **Kafka UI** | http://localhost:8081 | — |
| **MinIO Console** | http://localhost:9001 | minioadmin / minioadmin |

---

## 📡 API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `GET` | `/api/health` | System status and latest record time |
| `GET` | `/api/traffic/latest` | Latest record per location |
| `GET` | `/api/traffic/summary` | Aggregated stats for the past 1 hour |
| `GET` | `/api/weather/impact` | Speed vs weather condition analysis |
| `GET` | `/api/traffic/location/{name}` | History for a specific location |
| `GET` | `/api/traffic/chart/{name}` | 5-minute bucketed data for charting |

---

## 📁 Project Structure

```
traffic_project/
├── dags/
│   └── traffic_ingestion_dag.py   # Airflow DAG (runs every 3 min)
├── src/
│   ├── ai_worker/
│   │   └── main.py                # YOLOv8 vehicle counter
│   ├── ingestion/
│   │   ├── fetch_hcm_traffic.py   # Camera scraper
│   │   ├── fetch_tomtom.py        # TomTom API fetcher
│   │   └── fetch_weather.py       # OpenWeather fetcher
│   ├── serving/
│   │   ├── main.py                # FastAPI application
│   │   ├── models.py              # Pydantic schemas
│   │   ├── index.html             # Web dashboard
│   │   └── requirements.serving.txt
│   └── streaming/
│       └── spark_processor.py     # Spark Streaming join & write
├── Dockerfile.ai
├── Dockerfile.spark
├── Dockerfile.serving
├── docker-compose.yml
├── requirements.txt
├── schema.sql                     # Neon DB schema (run once)
└── .env.example
```

---

## ⚠️ Notes


- Spark joins three streams with a **5-minute watermark** — allow a few minutes for the first records to appear in the dashboard
- Camera URLs are public endpoints from the HCMC traffic authority and may change over time

---

## 📄 License

MIT License — feel free to use, modify, and distribute.