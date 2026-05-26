"""Integration tests for the aggregation job (windowed metrics + Gold merge)."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

from aggregation_job import aggregate_state_category
from utils.delta_utils import upsert_delta

_SILVER_SCHEMA = T.StructType(
    [
        T.StructField("customer_id", T.StringType()),
        T.StructField("customer_state", T.StringType()),
        T.StructField("product_category", T.StringType()),
        T.StructField("payment_value", T.DoubleType()),
        T.StructField("is_high_value", T.BooleanType()),
        T.StructField("event_type", T.StringType()),
        T.StructField("event_ts", T.TimestampType()),
    ]
)

_GOLD_MERGE = (
    "t.window_start = s.window_start AND t.window_end = s.window_end AND "
    "t.customer_state = s.customer_state AND t.product_category = s.product_category"
)


def _silver(spark: SparkSession, rows: list[tuple]) -> DataFrame:
    """Build a Silver-shaped DataFrame for aggregation."""
    return spark.createDataFrame(rows, schema=_SILVER_SCHEMA)


def test_window_aggregation_counts_orders(spark: SparkSession) -> None:
    """Each populated window counts every order in it."""
    ts = datetime(2024, 1, 1, 12, 0, 10)
    rows = [(f"c{i}", "SP", "beleza", 100.0, False, "ORDER_CREATED", ts) for i in range(5)]
    agg = aggregate_state_category(_silver(spark, rows))

    max_orders = agg.agg(F.max("total_orders").alias("m")).collect()[0]["m"]
    assert max_orders == 5


def test_window_aggregation_sums_revenue(spark: SparkSession) -> None:
    """Revenue is summed per window."""
    ts = datetime(2024, 1, 1, 12, 0, 10)
    rows = [
        ("c1", "RJ", "telefonia", 100.0, False, "ORDER_CREATED", ts),
        ("c2", "RJ", "telefonia", 200.0, False, "ORDER_CREATED", ts),
        ("c3", "RJ", "telefonia", 50.0, False, "ORDER_CREATED", ts),
    ]
    agg = aggregate_state_category(_silver(spark, rows))

    max_rev = agg.agg(F.max("total_revenue").alias("m")).collect()[0]["m"]
    assert max_rev == 350.0


def test_foreachbatch_merge_is_idempotent(
    spark: SparkSession, tmp_delta_path: str
) -> None:
    """Merging the same aggregated batch twice does not duplicate Gold rows."""
    ts = datetime(2024, 1, 1, 12, 0, 10)
    rows = [("c1", "MG", "esporte", 90.0, False, "ORDER_CREATED", ts)]
    agg = aggregate_state_category(_silver(spark, rows)).cache()

    upsert_delta(spark, agg, tmp_delta_path, _GOLD_MERGE)
    count_after_first = spark.read.format("delta").load(tmp_delta_path).count()
    upsert_delta(spark, agg, tmp_delta_path, _GOLD_MERGE)
    count_after_second = spark.read.format("delta").load(tmp_delta_path).count()

    assert count_after_first == count_after_second


def test_window_slide_produces_overlapping_results(spark: SparkSession) -> None:
    """A 1-min window sliding 30s places a single event in two windows."""
    ts = datetime(2024, 1, 1, 12, 0, 10)
    rows = [("c1", "SP", "beleza", 100.0, False, "ORDER_CREATED", ts)]
    agg = aggregate_state_category(_silver(spark, rows))

    windows = {r["window_start"] for r in agg.select("window_start").collect()}
    assert len(windows) == 2
