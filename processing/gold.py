"""
CrisisLens — Phase 2/3, gold layer (Delta, batch).

Reads the silver table and builds serving-ready GOLD marts:
  - gold/hazards : map-ready feed (events with coordinates), with a human-readable
                   severity_level and a geo_source flag (exact point vs zone centroid).
  - gold/summary : rollup counts by source + hazard_type + severity_level.

Gold is a BATCH recompute (overwrite) — idempotent, rebuilt from silver.

Run:  python processing/gold.py
"""

import json
import os
import sys

# Spark's Python workers must use the SAME interpreter as the driver. (Installing
# Ollama via brew pulled python@3.14 onto PATH; without this, the UDF worker picks
# up 3.14 and crashes with a version mismatch against this venv's 3.12.)
os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count, max as smax, coalesce, udf, lit
from pyspark.sql.types import ArrayType, DoubleType
from delta import configure_spark_with_delta_pip

SILVER_PATH = "lakehouse/silver"
GOLD_HAZARDS_PATH = "lakehouse/gold/hazards"
GOLD_SUMMARY_PATH = "lakehouse/gold/summary"
ZONE_LOOKUP_PATH = "reference/zone_centroids.json"


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

    # `severity` is on DIFFERENT scales per source: USGS = earthquake magnitude,
    # NWS = categorical level mapped to 1-4. Bucket each on its OWN scale.
    mag = col("severity")
    severity_level = (
        when(col("source") == "usgs",                  # earthquake magnitude scale
             when(mag >= 7.0, "extreme")
             .when(mag >= 6.0, "severe")
             .when(mag >= 4.5, "moderate")
             .when(mag >= 2.5, "minor")
             .otherwise("unknown"))
        .otherwise(                                     # NWS categorical scale (1-4)
             when(mag >= 4, "extreme")
             .when(mag >= 3, "severe")
             .when(mag >= 2, "moderate")
             .when(mag >= 1, "minor")
             .otherwise("unknown"))
    )
    enriched = silver.withColumn("severity_level", severity_level)

    # Geo-enrichment: many NWS alerts have no point geometry (they cover forecast
    # zones). Fill their coordinates from the zone -> centroid lookup built by
    # processing/build_zone_lookup.py, and tag whether a row is an exact point or
    # an approximate zone centroid.
    zone_lookup = json.load(open(ZONE_LOOKUP_PATH)) if os.path.exists(ZONE_LOOKUP_PATH) else {}
    bc = spark.sparkContext.broadcast(zone_lookup)

    @udf(ArrayType(DoubleType()))
    def zone_centroid(zones):
        for z in (zones or []):
            c = bc.value.get(z)
            if c:
                return [float(c[0]), float(c[1])]
        return None

    enriched = enriched.withColumn("_zc", zone_centroid(col("zones")))
    enriched = enriched.withColumn(
        "geo_source",
        when(col("lat").isNotNull(), lit("point"))
        .when(col("_zc").isNotNull(), lit("zone_centroid"))
        .otherwise(lit(None)),
    )
    enriched = (enriched
                .withColumn("lat", coalesce(col("lat"), col("_zc").getItem(0)))
                .withColumn("lon", coalesce(col("lon"), col("_zc").getItem(1)))
                .drop("_zc"))

    # MART 1 — map-ready hazard feed: only events that now have coordinates.
    hazards = (
        enriched
        .filter(col("lat").isNotNull() & col("lon").isNotNull())
        .select("event_id", "source", "hazard_type", "severity", "severity_level",
                "lat", "lon", "geo_source", "place", "occurred_at", "updated_at",
                "title", "url", "description")
    )
    hazards.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_HAZARDS_PATH)

    # MART 2 — analytics rollup.
    summary = (
        enriched.groupBy("source", "hazard_type", "severity_level")
        .agg(count("*").alias("event_count"),
             smax("occurred_at").alias("latest_event_at"))
    )
    summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(GOLD_SUMMARY_PATH)

    # Verify.
    total = silver.count()
    g = spark.read.format("delta").load(GOLD_HAZARDS_PATH)
    print("\n================ GOLD: hazards (map-ready feed) ================")
    print(f"map-ready hazards: {g.count()} of {total} silver events")
    g.groupBy("geo_source").count().show(truncate=False)
    (g.orderBy(col("severity").desc_nulls_last(), col("occurred_at").desc())
       .select("source", "hazard_type", "severity_level", "geo_source", "lat", "lon", "place")
       .show(8, truncate=55))

    print("================ GOLD: summary (rollup, top 12) ================")
    (spark.read.format("delta").load(GOLD_SUMMARY_PATH)
       .orderBy(col("event_count").desc())
       .show(12, truncate=False))

    spark.stop()


if __name__ == "__main__":
    main()
