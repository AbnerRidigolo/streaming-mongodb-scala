"""Integration tests for the enrichment job (stream-static join + derivations)."""

from __future__ import annotations

import time
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from enrichment_job import enrich

# Schema for the minimal Silver-input events used in these tests.
_EVENT_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.StringType()),
        T.StructField("customer_state", T.StringType()),
        T.StructField("payment_value", T.DoubleType()),
        T.StructField("event_ts", T.TimestampType()),
        T.StructField("event_type", T.StringType()),
    ]
)


def _events(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """Build a minimal events DataFrame for enrichment."""
    return spark.createDataFrame(rows, schema=_EVENT_SCHEMA)


def _cust_dim(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """Build a customers dimension WITHOUT customer_state (as the job does)."""
    return spark.createDataFrame(rows, schema=["customer_id", "customer_city"])


def test_stream_static_join_adds_city_column(
    spark: SparkSession, tmp_delta_path: str, sample_customers_df: DataFrame
) -> None:
    """The stream-static join attaches the customer city to each event."""
    cust_dim = sample_customers_df.drop("customer_state")
    events = _events(
        spark,
        [("cust_0001", "SP", 100.0, datetime(2024, 1, 1, 12, 0, 0), "ORDER_CREATED")],
    )
    enriched = enrich(events, cust_dim)

    assert "customer_city" in enriched.columns
    row = enriched.collect()[0]
    assert row["customer_city"] == "city_1"


def test_delivery_sla_tier_sp_is_d3(spark: SparkSession, tmp_delta_path: str) -> None:
    """São Paulo orders get the fastest SLA tier (D+3)."""
    events = _events(
        spark,
        [("c1", "SP", 100.0, datetime(2024, 1, 1, 9, 0, 0), "ORDER_CREATED")],
    )
    enriched = enrich(events, _cust_dim(spark, [("c1", "sao_paulo")]))
    assert enriched.collect()[0]["delivery_sla_tier"] == "D+3"


def test_delivery_sla_tier_other_is_d8(
    spark: SparkSession, tmp_delta_path: str
) -> None:
    """States outside SP/RJ/MG/ES fall back to D+8."""
    events = _events(
        spark,
        [("c1", "AM", 100.0, datetime(2024, 1, 1, 9, 0, 0), "ORDER_CREATED")],
    )
    enriched = enrich(events, _cust_dim(spark, [("c1", "manaus")]))
    assert enriched.collect()[0]["delivery_sla_tier"] == "D+8"


def test_revenue_bucket_low_below_50(
    spark: SparkSession, tmp_delta_path: str
) -> None:
    """Orders below R$50 are bucketed as 'low'."""
    events = _events(
        spark,
        [("c1", "RJ", 42.0, datetime(2024, 1, 1, 9, 0, 0), "ORDER_CREATED")],
    )
    enriched = enrich(events, _cust_dim(spark, [("c1", "rio")]))
    row = enriched.collect()[0]
    assert row["revenue_bucket"] == "low"
    assert row["is_high_value"] is False


def test_watermark_filters_late_events(
    spark: SparkSession, tmp_delta_path: str
) -> None:
    """A watermarked streaming aggregation drops events behind the watermark."""
    bronze = tmp_delta_path
    schema = T.StructType(
        [
            T.StructField("id", T.StringType()),
            T.StructField("event_ts", T.TimestampType()),
        ]
    )

    # Batch 1: on-time event at 12:00 (sets watermark to ~11:50).
    spark.createDataFrame(
        [("a", datetime(2024, 1, 1, 12, 0, 0))], schema=schema
    ).write.format("delta").mode("append").save(bronze)

    stream = (
        spark.readStream.format("delta")
        .load(bronze)
        .withWatermark("event_ts", "10 minutes")
        .groupBy(F.window("event_ts", "5 minutes"))
        .count()
        .select(F.col("window.start").alias("w_start"), "count")
    )
    query = (
        stream.writeStream.format("memory")
        .queryName("wm_test")
        .outputMode("update")
        .start()
    )
    try:
        query.processAllAvailable()

        # Batch 2: advance watermark to 12:10 and add a very late event at 11:00.
        spark.createDataFrame(
            [
                ("b", datetime(2024, 1, 1, 12, 20, 0)),
                ("late", datetime(2024, 1, 1, 11, 0, 0)),
            ],
            schema=schema,
        ).write.format("delta").mode("append").save(bronze)
        query.processAllAvailable()
        time.sleep(1)

        starts = {r["w_start"] for r in spark.sql("SELECT * FROM wm_test").collect()}
    finally:
        query.stop()

    # The 11:00 window is behind the watermark and must be dropped.
    assert datetime(2024, 1, 1, 11, 0, 0) not in starts
    assert datetime(2024, 1, 1, 12, 0, 0) in starts
