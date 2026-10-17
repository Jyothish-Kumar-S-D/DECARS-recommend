# Contributors:
2023BCS0004 : Amal V Pratish
2023BCS0016 : Jyothish Kumar S D
2023BCS0031 : Mahesh P Pai


------------------------------------------------------------------------------------------
# DECARS · Distributed E-Commerce Clickstream Analytics and Recommendation System\

### Real Apache Kafka + Real HDFS + Real Spark MLlib ALS + FastAPI + React
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Kafka](https://img.shields.io/badge/Apache-Kafka-231F20?logo=apachekafka)
![Spark](https://img.shields.io/badge/Apache-Spark_3.5-E25A1C?logo=apachespark)
![HDFS](https://img.shields.io/badge/Apache-Hadoop_3.2-66CCFF?logo=apache)
![FastAPI](https://img.shields.io/badge/FastAPI-0.110-009688?logo=fastapi)
![React](https://img.shields.io/badge/React-18-61DAFB?logo=react)

---

## Real Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  kafka-producer (Python)                                     │
│  Generates 2 events/sec → Apache Kafka topic "clickstream"   │
└───────────────────────┬──────────────────────────────────────┘
                        │ Real Kafka messages
                        ▼
┌──────────────────────────────────────────────────────────────┐
│  Spark Structured Streaming  (kafka_to_hdfs.py)              │
│  Consumes Kafka → writes Parquet to HDFS every 10s           │
└───────────────────────┬──────────────────────────────────────┘
                        │ Parquet files
                        ▼
┌──────────────────────────────────────────────────────────────┐
│  HDFS 3-node cluster  (NameNode + 3 DataNodes)               │
│  /data/clickstream    — raw events                           │
│  /models/als_latest   — trained ALS model                    │
│  /data/recommendations — pre-computed top-10 per user        │
└───────────────────────┬──────────────────────────────────────┘
                        │ Read by
                        ▼
┌──────────────────────────────────────────────────────────────┐
│  Spark MLlib ALS Training  (train_als.py)                    │
│  spark-submit → rank=50, λ=0.01, 20 iterations               │
│  Saves ALSModel to HDFS                                      │
└───────────────────────┬──────────────────────────────────────┘
                        │ Model
                        ▼
┌──────────────────────────────────────────────────────────────┐
│  FastAPI Backend  (main.py)                                  │
│  • SSE  → real Kafka → browser                               │
│  • GET  /api/recommend → HDFS ALS → Redis cache              │
│  • POST /api/train     → spark-submit                        │
│  • GET  /api/hdfs/status → real WebHDFS API                  │
│  • GET  /api/spark/status → real Spark REST API              │
└───────────────────────┬──────────────────────────────────────┘
                        │ HTTP + SSE
                        ▼
┌──────────────────────────────────────────────────────────────┐
│  React Frontend  (Vite + real API calls)                     │
│  Shows real HDFS stats, real Kafka events, real ALS recs     │
└──────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Requirements
- Docker Desktop (8 GB RAM allocated minimum)
- Node.js ≥ 18 (for local frontend dev only)
- Anthropic API key

### 1. Clone & configure
```bash
git clone https://github.com/Jyothish-Kumar-S-D/Big-Data-Project-.git
cd nexus-recommend
cp .env.example .env
# Edit .env → add ANTHROPIC_API_KEY
```

### 2. Start the full stack
```bash
docker-compose up -d
```
First launch downloads ~3 GB of images. Takes 3–5 minutes.

### 3. Initialise HDFS & Kafka
```bash
chmod +x scripts/init.sh
./scripts/init.sh
```

### 4. Open the services

| Service | URL |
|---------|-----|
| **React Dashboard** | http://localhost:3000 |
| **FastAPI Swagger** | http://localhost:8000/docs |
| **HDFS Web UI** | http://localhost:9870 |
| **Spark Master UI** | http://localhost:8082 |
| **Kafka UI** | http://localhost:8080 |

### 5. Collect data & train ALS
Wait ~2 minutes for the Kafka producer to generate events, then:
```bash
curl -X POST http://localhost:8000/api/train
```
Training takes ~1–3 minutes. Watch progress at http://localhost:8082.

---

## Project Structure

```
nexus-recommend/
├── docker-compose.yml          ← Full cluster definition
├── .env.example                ← Copy to .env
├── scripts/
│   └── init.sh                 ← Bootstrap HDFS + Kafka
│
├── kafka_producer/             ← Real Kafka event generator
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
│
├── spark_jobs/                 ← PySpark jobs (run on real Spark cluster)
│   ├── kafka_to_hdfs.py        ← Structured Streaming: Kafka → HDFS Parquet
│   ├── train_als.py            ← ALS training: HDFS → ALSModel
│   └── realtime_inference.py   ← Live inference stream
│
├── backend/                    ← FastAPI serving layer
│   ├── main.py
│   ├── requirements.txt
│   └── Dockerfile
│
└── frontend/                   ← React dashboard
    ├── src/
    │   ├── App.jsx             ← SSE consumer, real API calls
    │   ├── components/
    │   └── hooks/
    └── Dockerfile
```

---

## ALS Algorithm

Spark MLlib ALS minimises:

```
min Σ(u,i) c_ui(p_ui - xᵤᵀyᵢ)² + λ(Σᵤ||xᵤ||² + Σᵢ||yᵢ||²)
```

Where:
- `p_ui = 1` if user u interacted with item i
- `c_ui = 1 + α × r_ui` (confidence from interaction count)
- `xᵤ, yᵢ` = latent factor vectors (rank k=50)
- `λ = 0.01` = L2 regularisation

---

## Recommendation Priority

| Priority | Source | When |
|----------|--------|------|
| 1st | **Redis cache** | Same user+product within 60s |
| 2nd | **HDFS ALS model** | Model trained and user in training data |
| 3rd | **Claude API** | New/cold-start users or no model |

---