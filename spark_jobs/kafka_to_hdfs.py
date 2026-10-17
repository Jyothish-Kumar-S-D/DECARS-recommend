"""
spark_jobs/kafka_to_hdfs.py
────────────────────────────
Spark Structured Streaming job:
  1. Reads clickstream events from Kafka topic 'clickstream'
  2. Parses JSON into a structured DataFrame
  3. Writes micro-batches as Parquet files to HDFS

Run:
  spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    spark_jobs/kafka_to_hdfs.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, FloatType, TimestampType,
)

KAFKA_BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
KAFKA_TOPIC   = "clickstream"
HDFS_URL      = os.getenv("HDFS_URL", "hdfs://namenode:9000")
CHECKPOINT    = f"{HDFS_URL}/checkpoints/kafka_to_hdfs"
OUTPUT_PATH   = f"{HDFS_URL}/data/clickstream"

# ── Schema matching the Kafka producer payload ─────────────────────────────
EVENT_SCHEMA = StructType([
    StructField("event_id",     StringType(),  False),
    StructField("timestamp",    StringType(),  False),
    StructField("user_id",      IntegerType(), False),
    StructField("product_id",   IntegerType(), False),
    StructField("product_name", StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("price",        FloatType(),   True),
    StructField("action",       StringType(),  False),
    StructField("session_id",   StringType(),  True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("KafkaToHDFS-Clickstream")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # ── Read from Kafka ────────────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # ── Parse JSON value ───────────────────────────────────────────────────
    events = (
        raw
        .select(from_json(col("value").cast("string"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .drop("timestamp")
    )

    # ── Write Parquet to HDFS partitioned by date ──────────────────────────
    query = (
        events.writeStream
        .format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT)
        .partitionBy("action")           # partition by action type
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    print(f"[kafka_to_hdfs] Streaming → {OUTPUT_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
