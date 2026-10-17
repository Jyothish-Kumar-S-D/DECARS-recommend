"""
spark_jobs/realtime_inference.py
─────────────────────────────────
Spark Structured Streaming job that:
  1. Reads live events from Kafka
  2. Loads the pre-trained ALS model from HDFS
  3. Generates on-the-fly recommendations for each event's user
  4. Writes results back to a Kafka topic 'recommendations'

This enables sub-second recommendation latency for live users.
"""

import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.ml.recommendation import ALSModel

KAFKA_BROKERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
INPUT_TOPIC    = "clickstream"
OUTPUT_TOPIC   = "recommendations"
HDFS_URL       = os.getenv("HDFS_URL", "hdfs://namenode:9000")
MODEL_PATH     = f"{HDFS_URL}/models/als_latest"
CHECKPOINT     = f"{HDFS_URL}/checkpoints/realtime_inference"

EVENT_SCHEMA = StructType([
    StructField("event_id",   StringType(),  False),
    StructField("user_id",    IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("action",     StringType(),  False),
    StructField("timestamp",  StringType(),  False),
])


def main():
    spark = (
        SparkSession.builder
        .appName("BigDataProject-RealtimeInference")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Load trained model
    print(f"[inference] Loading ALS model from {MODEL_PATH} …")
    model = ALSModel.load(MODEL_PATH)

    # Read from Kafka
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    events = (
        raw
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .dropDuplicates(["user_id"])   # one rec per unique user per micro-batch
    )

    def process_batch(batch_df, epoch_id):
        if batch_df.isEmpty():
            return

        # ALS: recommend top 5 products for each user in this batch
        recs = model.recommendForUserSubset(batch_df.select("user_id"), 5)

        # Flatten and send to Kafka output topic
        recs_flat = recs.toJSON().collect()
        print(f"[inference] Epoch {epoch_id}: {len(recs_flat)} user recommendation sets generated")

    query = (
        events.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
