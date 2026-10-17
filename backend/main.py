"""
backend/main.py
────────────────
FastAPI application that:
  • /health              — liveness check
  • /api/stream          — SSE: live Kafka events pushed to browser
  • /api/hdfs/status     — real HDFS node stats via WebHDFS REST API
  • /api/spark/status    — Spark job & stage info via Spark REST API
  • /api/recommend       — ALS recommendations (HDFS pre-computed OR Claude fallback)
  • /api/train           — trigger a fresh ALS training job via spark-submit
"""

import asyncio
import json
import os
import subprocess
import time
from contextlib import asynccontextmanager
from typing import Optional

import httpx
import redis.asyncio as aioredis
from anthropic import AsyncAnthropic
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from kafka import KafkaConsumer
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession

# ── Config ────────────────────────────────────────────────────────────────────
HDFS_URL            = os.getenv("HDFS_URL", "hdfs://namenode:9000")
HDFS_WEBHDFS        = "http://namenode:9870/webhdfs/v1"
HDFS_JMX            = "http://namenode:9870/jmx"
KAFKA_BROKERS       = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
REDIS_URL           = os.getenv("REDIS_URL", "redis://redis:6379")
SPARK_MASTER        = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
SPARK_UI            = "http://spark-master:8080"
ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY", "")
MODEL_PATH          = f"{HDFS_URL}/models/als_latest"
RECS_PATH           = f"{HDFS_URL}/data/recommendations"
RECS_CACHE_TTL      = 60   # seconds
TRAIN_LOG_DIR       = os.getenv("TRAIN_LOG_DIR", "/tmp/decars_logs")
TRAIN_SCRIPT        = "/app/spark_jobs/train_als.py"

# Load products catalogue (with fallback)
try:
    with open("products.json", "r", encoding="utf-8") as file:
        PRODUCTS = json.load(file)
except Exception as e:
    print(f"[warning] Could not load products.json ({e}), using default catalog")
    PRODUCTS = [
        {"id": 1,  "name": "Sony WH-1000XM5",       "category": "Electronics",  "price": 349.0,  "img": "🎧", "rating": 4.8},
        {"id": 2,  "name": "MacBook Air M3",         "category": "Computers",    "price": 1299.0, "img": "💻", "rating": 4.9},
        {"id": 3,  "name": "Nike Air Max 270",       "category": "Footwear",     "price": 130.0,  "img": "👟", "rating": 4.6},
        {"id": 4,  "name": "Kindle Paperwhite",      "category": "Electronics",  "price": 139.0,  "img": "📖", "rating": 4.7},
        {"id": 5,  "name": "Instant Pot Duo 7-in-1", "category": "Kitchen",      "price": 99.0,   "img": "🍲", "rating": 4.8},
        {"id": 6,  "name": "Levi's 511 Slim Jeans",  "category": "Apparel",      "price": 79.0,   "img": "👖", "rating": 4.5},
        {"id": 7,  "name": "Fitbit Charge 6",        "category": "Wearables",    "price": 159.0,  "img": "⌚", "rating": 4.4},
        {"id": 8,  "name": "Canon EOS R50",          "category": "Photography",  "price": 679.0,  "img": "📷", "rating": 4.7},
        {"id": 9,  "name": "LEGO Technic F40",       "category": "Toys",         "price": 189.0,  "img": "🧱", "rating": 4.9},
        {"id": 10, "name": "Dyson V15 Detect",       "category": "Home",         "price": 649.0,  "img": "🌀", "rating": 4.6},
        {"id": 11, "name": "Vitamix 5200",           "category": "Kitchen",      "price": 449.0,  "img": "🥤", "rating": 4.8},
        {"id": 12, "name": "Samsung 4K QLED 65\"",   "category": "Electronics",  "price": 1199.0, "img": "📺", "rating": 4.7},
    ]

# ── Global singletons ─────────────────────────────────────────────────────────
redis_client: Optional[aioredis.Redis] = None
spark:        Optional[SparkSession]   = None
als_model:    Optional[ALSModel]       = None
anthropic_client = AsyncAnthropic(api_key=ANTHROPIC_API_KEY) if ANTHROPIC_API_KEY else None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global redis_client, spark, als_model

    # Redis
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

    # Spark session (lightweight — used only for model loading + inference)
    spark = (
        SparkSession.builder
        .appName("DecarsRecommend-API")
        .master(SPARK_MASTER)
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Try to load existing ALS model
    try:
        als_model = ALSModel.load(MODEL_PATH)
        print("[api] ALS model loaded from HDFS ✅")
    except Exception as e:
        print(f"[api] No model yet ({e}) — run /api/train first")

    yield

    await redis_client.aclose()
    spark.stop()


app = FastAPI(title="DECARS·RECOMMEND API", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Health ────────────────────────────────────────────────────────────────────
@app.get("/health")
async def health():
    return {
        "status":    "ok",
        "model":     als_model is not None,
        "redis":     await redis_client.ping(),
        "timestamp": time.time(),
    }


# ── SSE: Live Kafka stream ────────────────────────────────────────────────────
@app.get("/api/stream")
async def kafka_stream():
    """
    Server-Sent Events endpoint.
    Browser subscribes here and receives real Kafka events in real time.
    """
    def event_generator():
        consumer = KafkaConsumer(
            "clickstream",
            bootstrap_servers=KAFKA_BROKERS,
            auto_offset_reset="latest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=30000,
            group_id=f"sse-{time.time()}",
        )
        for message in consumer:
            event = message.value
            yield f"data: {json.dumps(event)}\n\n"
        consumer.close()

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# ── HDFS Status ───────────────────────────────────────────────────────────────
@app.get("/api/hdfs/status")
async def hdfs_status():
    """Query real HDFS NameNode via WebHDFS REST API."""
    async with httpx.AsyncClient(timeout=5) as client:
        try:
            # Overall cluster stats
            r = await client.get(f"{HDFS_WEBHDFS}/?op=GETCONTENTSUMMARY", params={"path": "/"})
            summary = r.json() if r.status_code == 200 else {}

            # Live datanode info (NameNodeInfo) + capacity/blocks (FSNamesystem)
            beans = []
            for mbean in (
                "Hadoop:service=NameNode,name=NameNodeInfo",
                "Hadoop:service=NameNode,name=FSNamesystem",
            ):
                jmx_r = await client.get(HDFS_JMX, params={"qry": mbean})
                if jmx_r.status_code == 200:
                    beans.extend(jmx_r.json().get("beans", []))

            return {"status": "ok", "summary": summary, "jmx": {"beans": beans}}
        except Exception as e:
            return {"status": "error", "detail": str(e)}


# ── Spark Status ──────────────────────────────────────────────────────────────
@app.get("/api/spark/status")
async def spark_status():
    """Query real Spark Master REST API for job and worker info."""
    # /json/ (trailing slash) is the Master REST endpoint; /json issues a 302.
    async with httpx.AsyncClient(timeout=5, follow_redirects=True) as client:
        try:
            r = await client.get(f"{SPARK_UI}/json/")
            if r.status_code != 200 or "json" not in r.headers.get("content-type", ""):
                return {"status": "error", "detail": f"Spark Master UI returned {r.status_code}"}

            master = r.json()
            applications = master.get("activeapps", []) + master.get("completedapps", [])
            return {
                "status":       "ok",
                "applications": applications,
                "workers":      master,
            }
        except Exception as e:
            return {"status": "error", "detail": str(e)}


# ── Recommendations ───────────────────────────────────────────────────────────
@app.get("/api/recommend")
async def recommend(
    user_id:    int            = Query(..., description="User ID"),
    product_id: Optional[int]  = Query(None, description="Currently viewed product (optional)"),
    segment:    Optional[str]  = Query(None, description="User segment label"),
):
    """
    1. Check Redis cache first.
    2. If ALS model available: load pre-computed recs from HDFS.
    3. Fallback: ask Claude API to simulate ALS recommendations.
    """
    cache_key = f"recs:{user_id}:{product_id or 'none'}"

    # ── Cache hit ──────────────────────────────────────────────────────────
    cached = await redis_client.get(cache_key)
    if cached:
        return {"source": "redis_cache", "recommendations": json.loads(cached)}

    # ── ALS model (HDFS pre-computed) ──────────────────────────────────────
    if als_model is not None:
        try:
            recs_df = spark.read.parquet(RECS_PATH)
            user_recs = (
                recs_df
                .filter(recs_df.user_id == user_id)
                .limit(1)
                .collect()
            )
            if user_recs:
                raw_recs = user_recs[0]["recommendations"]   # list of (product_id, rating)
                enriched = []
                for r in raw_recs[:4]:
                    p = next((x for x in PRODUCTS if x["id"] == r["product_id"]), None)
                    if p:
                        enriched.append({**p, "score": float(r["rating"]), "reason": "ALS collaborative filtering", "source": "als_hdfs"})
                if enriched:
                    await redis_client.setex(cache_key, RECS_CACHE_TTL, json.dumps(enriched))
                    return {"source": "als_hdfs", "recommendations": enriched}
        except Exception as e:
            print(f"[api] ALS lookup failed: {e}")

    # ── Claude fallback ────────────────────────────────────────────────────
    if anthropic_client:
        catalog = "\n".join(f"#{p['id']} {p['name']} ({p['category']}, ${p['price']}, ⭐{p['rating']})" for p in PRODUCTS)
        viewing = next((p for p in PRODUCTS if p["id"] == product_id), None)
        prompt = f"""You are an ALS-based collaborative filtering engine.
User ID: {user_id}, Segment: {segment or 'General'}
{f"Currently viewing: {viewing['name']} ({viewing['category']}, ${viewing['price']})" if viewing else ""}

Product catalog:
{catalog}

Return exactly 4 recommendations as JSON array. Each: {{"id": int, "score": float 0-1, "reason": "8-word reason"}}
Sort by score descending. Return ONLY the JSON array."""

        response = await anthropic_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=400,
            messages=[{"role": "user", "content": prompt}],
        )
        raw  = response.content[0].text.strip().replace("```json", "").replace("```", "").strip()
        parsed = json.loads(raw)
        enriched = []
        for r in parsed:
            p = next((x for x in PRODUCTS if x["id"] == r["id"]), None)
            if p:
                enriched.append({**p, "score": r["score"], "reason": r["reason"], "source": "claude_fallback"})

        await redis_client.setex(cache_key, RECS_CACHE_TTL, json.dumps(enriched))
        return {"source": "claude_fallback", "recommendations": enriched}

    # ── Nothing to serve ───────────────────────────────────────────────────
    if als_model is not None:
        return {
            "source": "none",
            "recommendations": [],
            "error": f"User {user_id} is not in the trained ALS data yet",
        }

    return {"source": "none", "recommendations": [], "error": "No model and no API key configured"}


# ── Trigger ALS Training ──────────────────────────────────────────────────────
# Spark-submit output is teed to a log file so the dashboard can show what the
# job produced (RMSE, saved paths) instead of discarding it into an unread pipe.
training: dict = {"proc": None, "pid": None, "started_at": None, "log_path": None}


def train_summary(log_text: str) -> dict:
    """Pull the useful result lines out of the driver log."""
    import re

    def grab(pattern: str):
        m = re.search(pattern, log_text)
        return m.group(1) if m else None

    rmse = grab(r"RMSE = ([\d.]+)")
    return {
        "interactions": grab(r"Total interactions: (\d+)"),
        "rmse":         float(rmse) if rmse else None,
        "model_path":   grab(r"Model saved → (\S+)"),
        "recs_path":    grab(r"Recommendations saved → (\S+)"),
        "no_data":      "No data yet" in log_text,
    }


def read_log(lines: int = 25) -> str:
    path = training.get("log_path")
    if not path or not os.path.exists(path):
        return ""
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        return "".join(fh.readlines()[-lines:])


@app.post("/api/train")
async def trigger_training():
    """
    Submits the ALS training job to the real Spark cluster via spark-submit.
    Returns immediately; training runs asynchronously — follow it with
    GET /api/train/status.
    """
    proc = training.get("proc")
    if proc is not None and proc.poll() is None:
        return {
            "status":     "already_running",
            "pid":        training["pid"],
            "started_at": training["started_at"],
            "message":    "An ALS training job is already running",
        }

    os.makedirs(TRAIN_LOG_DIR, exist_ok=True)
    log_path = f"{TRAIN_LOG_DIR}/train_{int(time.time())}.log"

    cmd = [
        "spark-submit",
        "--master", SPARK_MASTER,
        "--driver-memory", "2g",
        "--executor-memory", "4g",
        "--executor-cores", "2",
        TRAIN_SCRIPT,
    ]

    # Keep a handle on the log so the child can write freely (an undrained PIPE
    # would block the driver once the 64 KB buffer fills up).
    try:
        log_file = open(log_path, "w", encoding="utf-8", buffering=1)
        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd="/app",
        )
    except Exception as e:
        return {"status": "error", "detail": f"Could not start spark-submit: {e}"}

    training.update({
        "proc": proc,
        "pid": proc.pid,
        "started_at": time.time(),
        "log_path": log_path,
        "log_file": log_file,
    })

    return {
        "status":  "submitted",
        "pid":     proc.pid,
        "command": " ".join(cmd),
        "log_path": log_path,
    }


@app.get("/api/train/status")
async def training_status(lines: int = Query(25, ge=1, le=200)):
    """Progress + result of the latest spark-submit run triggered from the UI."""
    proc     = training.get("proc")
    running  = proc is not None and proc.poll() is None
    log_text = read_log(lines)

    return {
        "running":    running,
        "pid":        training.get("pid"),
        "started_at": training.get("started_at"),
        "exit_code":  None if running or proc is None else proc.returncode,
        "log_path":   training.get("log_path"),
        "summary":    train_summary(read_log(1000)),
        "log_tail":   log_text,
    }


# ── Kafka topic stats ─────────────────────────────────────────────────────────
@app.get("/api/kafka/stats")
async def kafka_stats():
    """Return offset lag and message counts from the clickstream topic."""
    from kafka import KafkaAdminClient
    from kafka.structs import TopicPartition
    try:
        admin  = KafkaAdminClient(bootstrap_servers=KAFKA_BROKERS)
        topics = admin.list_topics()
        admin.close()
        return {"topics": topics, "clickstream_active": "clickstream" in topics}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
