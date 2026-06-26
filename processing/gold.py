"""
CrisisLens — Phase 2, gold layer (Delta, batch).

Reads the silver table and builds serving-ready GOLD marts:
  - gold/hazards : map-ready feed (events that have coordinates), with a
                   human-readable severity_level — what Phase 3's API/map serve.
  - gold/summary : rollup counts by source + hazard_type + severity_level.

Gold is a BATCH recompute (overwrite). Bronze/silver are the streaming layers;
gold marts are rebuilt from silver (in production, on a schedule). Overwrite makes
the job idempotent: re-running always reproduces the same marts from silver.

Run:  python processing/gold.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, max as smax
from delta import configure_spark_with_delta_pip

SILVER_PATH = "lakehouse/silver"
GOLD_HAZARDS_PATH = "lakehouse/gold/hazards"
GOLD_SUMMARY_PATH = "lakehouse/gold/summary"


def build_spark():
    builder = (
        SparkSession.builder.appName("crisislens-gold")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    silver = spark.read.format("delta").load(SILVER_PATH)

    # Coarse, human-readable severity bucket. Works across both sources for now;
    # true cross-source impact scoring is the Phase 3 severity-triage model.
    severity_level = (
        when(col("severity") >= 4, "extreme")
        .when(col("severity") >= 3, "severe")
        .when(col("severity") >= 2, "moderate")
        .when(col("severity") >= 1, "minor")
        .otherwise("unknown")
    )
    enriched = silver.withColumn("severity_level", severity_level)

    # MART 1 — map-ready hazard feed: only events that have coordinates to plot.
    hazards = (
        enriched
        .filter(col("lat").isNotNull() & col("lon").isNotNull())
        .select("event_id", "source", "hazard_type", "severity", "severity_level",
                "lat", "lon", "place", "occurred_at", "updated_at",
                "title", "url", "description")
    )
    hazards.write.format("delta").mode("overwrite").save(GOLD_HAZARDS_PATH)

    # MART 2 — analytics rollup: counts by source / hazard_type / severity_level.
    summary = (
        enriched.groupBy("source", "hazard_type", "severity_level")
        .agg(count("*").alias("event_count"),
             smax("occurred_at").alias("latest_event_at"))
    )
    summary.write.format("delta").mode("overwrite").save(GOLD_SUMMARY_PATH)

    # Verify.
    total = silver.count()
    g = spark.read.format("delta").load(GOLD_HAZARDS_PATH)
    print("\n================ GOLD: hazards (map-ready feed) ================")
    print(f"map-ready hazards: {g.count()} of {total} silver events have coordinates")
    (g.orderBy(col("severity").desc_nulls_last(), col("occurred_at").desc())
       .select("source", "hazard_type", "severity_level", "lat", "lon", "place", "title")
       .show(8, truncate=55))

    print("================ GOLD: summary (rollup, top 12) ================")
    (spark.read.format("delta").load(GOLD_SUMMARY_PATH)
       .orderBy(col("event_count").desc())
       .show(12, truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
