"""
CrisisLens — Phase 2, bronze ingestion (Spark Structured Streaming).

Reads BOTH Kafka topics (earthquakes, weather-alerts) and lands every record —
raw JSON payload + Kafka metadata — into a Delta 'bronze' table.

Bronze = an exact, append-only copy of what arrived. No parsing, no cleaning.
Silver (typed/cleaned/deduped) and gold (aggregates) come next. Keeping the raw
payload means we can always re-derive silver/gold if our logic changes — the same
"keep the durable log" instinct Kafka taught you, now at the storage layer.

trigger(availableNow=True): process everything currently in Kafka, then stop.
A checkpoint records which offsets were consumed, so re-running ingests ONLY new
records (incremental, exactly-once) — not the whole backlog again.

Run:  python processing/bronze_stream.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from delta import configure_spark_with_delta_pip

KAFKA_BOOTSTRAP = "localhost:9092"
TOPICS = "earthquakes,weather-alerts"
BRONZE_PATH = "lakehouse/bronze"
CHECKPOINT_PATH = "lakehouse/_checkpoints/bronze"
SPARK_VERSION = "3.5.8"  # must match pyspark; selects the matching Kafka connector


def build_spark():
    builder = (
        SparkSession.builder.appName("crisislens-bronze")
        # Turn on Delta Lake's SQL extensions + catalog.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    # On first run Spark downloads two jars from Maven: the Delta libs and the
    # Kafka source connector. (Takes a minute; cached afterward.)
    return configure_spark_with_delta_pip(
        builder,
        extra_packages=[f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"],
    ).getOrCreate()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")  # quiet Spark's verbose INFO logs

    # 1) Kafka as a STREAMING source. Each Kafka record becomes a row with
    #    columns: key, value (both bytes), topic, partition, offset, timestamp.
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "earliest")  # read history on first run; checkpoint takes over after
        .load()
    )

    # 2) Bronze keeps the payload RAW (just decode bytes -> string) plus the
    #    Kafka metadata, and stamps when Spark ingested it.
    bronze = raw.select(
        col("key").cast("string").alias("event_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
        current_timestamp().alias("bronze_ingested_at"),
    )

    # 3) Write the stream to a Delta table. append = only add new rows.
    query = (
        bronze.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)   # drain current backlog, then stop
        .start(BRONZE_PATH)
    )
    query.awaitTermination()

    # 4) Verify: read the Delta table back as a normal (batch) DataFrame.
    df = spark.read.format("delta").load(BRONZE_PATH)
    print("\n================ BRONZE TABLE ================")
    print("total rows:", df.count())
    df.groupBy("topic").count().show(truncate=False)
    df.select("topic", "event_key", "kafka_timestamp").show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
