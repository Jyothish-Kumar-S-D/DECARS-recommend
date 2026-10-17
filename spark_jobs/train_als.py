"""
spark_jobs/train_als.py
────────────────────────
Real Spark MLlib ALS training pipeline:
  1. Reads clickstream Parquet from HDFS
  2. Computes implicit interaction weights per (user, product)
  3. Trains ALS collaborative filtering model
  4. Evaluates on held-out split (RMSE)
  5. Saves model + user/item factor matrices back to HDFS
  6. Writes top-N recommendations per user to HDFS

Run:
  spark-submit \
    --master spark://spark-master:7077 \
    --driver-memory 2g \
    --executor-memory 4g \
    spark_jobs/train_als.py
"""

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit, sum as _sum
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator

HDFS_URL   = os.getenv("HDFS_URL", "hdfs://namenode:9000")
INPUT_PATH = f"{HDFS_URL}/data/clickstream"
MODEL_PATH = f"{HDFS_URL}/models/als_latest"
RECS_PATH  = f"{HDFS_URL}/data/recommendations"

# ALS hyperparameters
RANK           = 50
MAX_ITER       = 20
REG_PARAM      = 0.01
ALPHA          = 40.0    # confidence scaling for implicit feedback
TOP_N          = 10      # recommendations per user
IMPLICIT_PREFS = True    # use implicit feedback (clicks / views)


# ── Action → confidence weight ─────────────────────────────────────────────
ACTION_WEIGHT = {
    "view":         1.0,
    "click":        2.0,
    "add_to_cart":  4.0,
    "wishlist":     3.0,
    "purchase":     8.0,
}


def build_interaction_matrix(spark, path: str):
    """
    Read raw events and convert to (user_id, product_id, rating) triples.
    Rating = sum of weighted interactions (implicit feedback).
    """
    df = spark.read.parquet(path)

    # Map action strings to numeric weights using a CASE expression
    weighted = df.withColumn(
        "weight",
        when(col("action") == "purchase",    ACTION_WEIGHT["purchase"])
        .when(col("action") == "add_to_cart", ACTION_WEIGHT["add_to_cart"])
        .when(col("action") == "wishlist",    ACTION_WEIGHT["wishlist"])
        .when(col("action") == "click",       ACTION_WEIGHT["click"])
        .otherwise(ACTION_WEIGHT["view"])
    )

    # Aggregate: sum weights per (user, product) = implicit rating
    interactions = (
        weighted.groupBy("user_id", "product_id")
        .agg(_sum("weight").alias("rating"))
    )

    print(f"[train_als] Total interactions: {interactions.count()}")
    return interactions


def train(spark, interactions):
    """Train ALS model on the interaction matrix."""
    train_df, test_df = interactions.randomSplit([0.8, 0.2], seed=42)

    als = ALS(
        rank=RANK,
        maxIter=MAX_ITER,
        regParam=REG_PARAM,
        alpha=ALPHA,
        implicitPrefs=IMPLICIT_PREFS,
        userCol="user_id",
        itemCol="product_id",
        ratingCol="rating",
        coldStartStrategy="drop",   # handle unseen users/items
        nonnegative=True,
    )

    print(f"[train_als] Training ALS (rank={RANK}, iter={MAX_ITER}, λ={REG_PARAM}) …")
    model = als.fit(train_df)

    # Evaluate
    predictions = model.transform(test_df)
    evaluator   = RegressionEvaluator(
        metricName="rmse", labelCol="rating", predictionCol="prediction"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"[train_als] ✅  RMSE = {rmse:.4f}")

    return model, rmse


def save_recommendations(model, recs_path: str):
    """Generate top-N recs for all users and save to HDFS."""
    recs = model.recommendForAllUsers(TOP_N)
    recs.write.mode("overwrite").parquet(recs_path)
    print(f"[train_als] Recommendations saved → {recs_path}")


def main():
    spark = (
        SparkSession.builder
        .appName("BigDataProject-ALSTraining")
        .config("spark.sql.shuffle.partitions", "128")
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # 1. Build interaction matrix from HDFS clickstream data
    interactions = build_interaction_matrix(spark, INPUT_PATH)

    if interactions.count() == 0:
        print("[train_als] ⚠️  No data yet — run the Kafka producer first.")
        sys.exit(0)

    # 2. Train ALS
    model, rmse = train(spark, interactions)

    # 3. Save model to HDFS
    model.write().overwrite().save(MODEL_PATH)
    print(f"[train_als] Model saved → {MODEL_PATH}")

    # 4. Pre-compute top-N recs for all users
    save_recommendations(model, RECS_PATH)

    print(f"[train_als] Done. RMSE={rmse:.4f}")
    spark.stop()


if __name__ == "__main__":
    main()
