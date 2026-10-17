"""
kafka_producer/producer.py
──────────────────────────
Real Apache Kafka producer that emits e-commerce clickstream events.
Events are JSON-serialised and sent to the 'clickstream' topic.

Event schema:
{
  "event_id":   "uuid4",
  "timestamp":  "ISO-8601",
  "user_id":    int,
  "product_id": int,
  "action":     "view|click|add_to_cart|purchase|wishlist",
  "session_id": "uuid4",
  "price":      float,
  "category":   str
}
"""

import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ── Config ────────────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC             = os.getenv("KAFKA_TOPIC", "clickstream")
EVENT_RATE_MS     = int(os.getenv("EVENT_RATE_MS", "500"))

# ── Synthetic catalogue ───────────────────────────────────────────────────────
PRODUCTS = [
    {"id": 1,  "name": "Sony WH-1000XM5",       "category": "Electronics",  "price": 349.00},
    {"id": 2,  "name": "MacBook Air M3",         "category": "Computers",    "price": 1299.00},
    {"id": 3,  "name": "Nike Air Max 270",       "category": "Footwear",     "price": 130.00},
    {"id": 4,  "name": "Kindle Paperwhite",      "category": "Electronics",  "price": 139.00},
    {"id": 5,  "name": "Instant Pot Duo 7-in-1", "category": "Kitchen",      "price": 99.00},
    {"id": 6,  "name": "Levi's 511 Slim Jeans",  "category": "Apparel",      "price": 79.00},
    {"id": 7,  "name": "Fitbit Charge 6",        "category": "Wearables",    "price": 159.00},
    {"id": 8,  "name": "Canon EOS R50",          "category": "Photography",  "price": 679.00},
    {"id": 9,  "name": "LEGO Technic F40",       "category": "Toys",         "price": 189.00},
    {"id": 10, "name": "Dyson V15 Detect",       "category": "Home",         "price": 649.00},
    {"id": 11, "name": "Vitamix 5200",           "category": "Kitchen",      "price": 449.00},
    {"id": 12, "name": 'Samsung 4K QLED 65"',    "category": "Electronics",  "price": 1199.00},
]

# Weighted actions — views are much more common than purchases
ACTIONS        = ["view", "view", "view", "view", "click", "click", "add_to_cart", "purchase", "wishlist"]
NUM_USERS      = 500      # simulate a pool of 500 unique users
NUM_SESSIONS   = 200      # active sessions at any time


def make_producer() -> KafkaProducer:
    """Retry until broker is available."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: str(k).encode("utf-8"),
                acks="all",                  # wait for all replicas
                retries=5,
                linger_ms=10,               # small batching window
                compression_type="gzip",
            )
            print(f"[producer] Connected to Kafka at {BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print("[producer] Broker not ready — retrying in 5s…")
            time.sleep(5)


def generate_event(session_id: str, user_id: int) -> dict:
    product = random.choice(PRODUCTS)
    action  = random.choice(ACTIONS)
    return {
        "event_id":   str(uuid.uuid4()),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "user_id":    user_id,
        "product_id": product["id"],
        "product_name": product["name"],
        "category":   product["category"],
        "price":      product["price"],
        "action":     action,
        "session_id": session_id,
    }


def main():
    producer = make_producer()
    sessions = {str(uuid.uuid4()): random.randint(1, NUM_USERS) for _ in range(NUM_SESSIONS)}

    print(f"[producer] Streaming to topic '{TOPIC}' every {EVENT_RATE_MS} ms …")
    sent = 0

    while True:
        session_id, user_id = random.choice(list(sessions.items()))
        event = generate_event(session_id, user_id)

        # Use user_id as Kafka partition key → same user always same partition
        producer.send(TOPIC, key=event["user_id"], value=event)
        sent += 1

        if sent % 100 == 0:
            producer.flush()
            print(f"[producer] {sent} events sent  |  latest: user={user_id} action={event['action']} product={event['product_name']}")

        time.sleep(EVENT_RATE_MS / 1000)


if __name__ == "__main__":
    main()
