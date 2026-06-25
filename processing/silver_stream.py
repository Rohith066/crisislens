"""
CrisisLens — Phase 2, silver layer (Spark Structured Streaming + Delta MERGE).

Reads the bronze Delta table as a STREAM, parses the raw JSON into typed columns,
applies quality filters, and UPSERTS into a silver Delta table keyed by event_id —
keeping the latest version of each event. (USGS revises quakes in place, so bronze
holds multiple versions of one event_id; silver must keep only the newest.)

The upsert is a Delta MERGE inside foreachBatch — the idempotent
"update-if-exists-else-insert" pattern at the heart of a lakehouse.

Run:  python processing/silver_stream.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

BRONZE_PATH = "lakehouse/bronze"
SILVER_PATH = "lakehouse/silver"
CHECKPOINT_PATH = "lakehouse/_checkpoints/silver"

# Schema of the canonical JSON the producers wrote into `raw_json`.
EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("source", StringType()),
    StructField("hazard_type", StringType()),
    StructField("title", StringType()),
    StructField("severity", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("lon", DoubleType()),
    StructField("place", StringType()),
    StructField("occurred_at", StringType()),
    StructField("updated_at", StringType()),
    StructField("ingested_at", StringType()),
    StructField("url", StringType()),
    StructField("description", StringType()),
])

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"  # matches what the producers emit (UTC, 'Z' suffix)


def build_spark():
    builder = (
        SparkSession.builder.appName("crisislens-silver")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")  # interpret/show times as UTC
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def upsert_to_silver(micro_batch_df, batch_id):
    """Runs once per micro-batch: collapse to the newest row per event_id, then
    MERGE into the silver table (update if newer, insert if new)."""
    spark = micro_batch_df.sparkSession

    # 1) Within this batch, keep only the newest row per event_id.
    newest = Window.partitionBy("event_id").orderBy(col("updated_at").desc())
    deduped = (
        micro_batch_df
        .withColumn("_rn", row_number().over(newest))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )

    # 2) First run -> create the table. Afterwards -> MERGE into it.
    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        deduped.write.format("delta").save(SILVER_PATH)
        return

    silver = DeltaTable.forPath(spark, SILVER_PATH)
    (
        silver.alias("t")
        .merge(deduped.alias("s"), "t.event_id = s.event_id")
        .whenMatchedUpdateAll(condition="s.updated_at >= t.updated_at")  # newer version wins
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Bronze is itself a streaming source — medallion layers chain via streams.
    bronze = spark.readStream.format("delta").load(BRONZE_PATH)

    parsed = (
        bronze.select(from_json(col("raw_json"), EVENT_SCHEMA).alias("e")).select("e.*")
        .withColumn("occurred_at", to_timestamp("occurred_at", TS_FORMAT))
        .withColumn("updated_at", to_timestamp("updated_at", TS_FORMAT))
        .withColumn("ingested_at", to_timestamp("ingested_at", TS_FORMAT))
        # --- quality gates ---
        .filter(col("event_id").isNotNull())            # drop rows that failed to parse
        .filter(col("hazard_type") != "test message")   # drop NWS test/keepalive messages
    )

    query = (
        parsed.writeStream
        .foreachBatch(upsert_to_silver)   # custom per-batch upsert logic
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    # Verify.
    df = spark.read.format("delta").load(SILVER_PATH)
    print("\n================ SILVER TABLE ================")
    print("distinct events:", df.count())
    df.groupBy("source", "hazard_type").count().orderBy(col("count").desc()).show(12, truncate=False)
    df.select("source", "event_id", "severity", "lat", "lon", "occurred_at", "title").show(5, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
