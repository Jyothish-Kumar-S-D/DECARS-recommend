#!/usr/bin/env bash
# scripts/init.sh
# ─────────────────────────────────────────────────────────────────────────────
# Run ONCE after `docker-compose up -d` to:
#   1. Create HDFS directory structure
#   2. Upload product catalogue to HDFS
#   3. Create Kafka topics with correct partitions
# ─────────────────────────────────────────────────────────────────────────────
set -e

echo "Waiting for NameNode to be ready…"
until curl -sf http://localhost:9870 > /dev/null; do sleep 3; done
echo "NameNode is up"

echo "Creating HDFS directories…"
docker exec namenode hdfs dfs -mkdir -p /data/clickstream
docker exec namenode hdfs dfs -mkdir -p /data/recommendations
docker exec namenode hdfs dfs -mkdir -p /models
docker exec namenode hdfs dfs -mkdir -p /checkpoints
docker exec namenode hdfs dfs -chmod -R 777 /data /models /checkpoints
echo "HDFS directories created"

echo "Uploading product catalogue to HDFS…"
cat > /tmp/products.json << 'JSON'
{"id":1,"name":"Sony WH-1000XM5","category":"Electronics","price":349,"rating":4.8}
{"id":2,"name":"MacBook Air M3","category":"Computers","price":1299,"rating":4.9}
{"id":3,"name":"Nike Air Max 270","category":"Footwear","price":130,"rating":4.6}
{"id":4,"name":"Kindle Paperwhite","category":"Electronics","price":139,"rating":4.7}
{"id":5,"name":"Instant Pot Duo 7-in-1","category":"Kitchen","price":99,"rating":4.8}
{"id":6,"name":"Levi's 511 Slim Jeans","category":"Apparel","price":79,"rating":4.5}
{"id":7,"name":"Fitbit Charge 6","category":"Wearables","price":159,"rating":4.4}
{"id":8,"name":"Canon EOS R50","category":"Photography","price":679,"rating":4.7}
{"id":9,"name":"LEGO Technic F40","category":"Toys","price":189,"rating":4.9}
{"id":10,"name":"Dyson V15 Detect","category":"Home","price":649,"rating":4.6}
{"id":11,"name":"Vitamix 5200","category":"Kitchen","price":449,"rating":4.8}
{"id":12,"name":"Samsung 4K QLED 65","category":"Electronics","price":1199,"rating":4.7}
JSON
docker cp /tmp/products.json namenode:/tmp/products.json
docker exec namenode hdfs dfs -put -f /tmp/products.json /data/products.json
echo "Catalogue uploaded to hdfs:///data/products.json"

echo "Waiting for Kafka broker…"
until docker exec kafka kafka-topics --bootstrap-server kafka:29092 --list > /dev/null 2>&1; do sleep 3; done
echo "Kafka is up"

echo "Creating Kafka topics…"
docker exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic clickstream \
  --partitions 12 \
  --replication-factor 1

docker exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic recommendations \
  --partitions 4 \
  --replication-factor 1
echo "Kafka topics created"

echo ""
echo "═══════════════════════════════════════════════════"
echo "Cluster initialised!"
echo ""
echo " Services:"
echo "  Frontend      →  http://localhost:3000"
echo "  Backend API   →  http://localhost:8000/docs"
echo "  HDFS Web UI   →  http://localhost:9870"
echo "  Spark UI      →  http://localhost:8082"
echo "  Kafka UI      →  http://localhost:8080"
echo ""
echo "  Next step: POST http://localhost:8000/api/train"
echo "  (waits ~2 min for enough clickstream data first)"
echo "═══════════════════════════════════════════════════"