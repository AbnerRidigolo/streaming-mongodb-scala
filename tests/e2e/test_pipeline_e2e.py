"""End-to-end test: synthetic events flow Bronze → Silver → Gold.

Drives the real enrichment and aggregation transformations through Structured
Streaming using the ``availableNow`` trigger (a deterministic single sweep of
all available data, equivalent to ``processAllAvailable``).
"""

from __future__ import annotations

from typing import Any, Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from aggregation_job import aggregate_state_category
from enrichment_job import enrich
from utils.delta_utils import upsert_delta

_GOLD_MERGE = (
    "t.window_start = s.window_start AND t.window_end = s.window_end AND "
    "t.customer_state = s.customer_state AND t.product_category = s.product_category"
)


def test_event_flows_from_bronze_to_gold(
    spark: SparkSession,
    tmp_delta_path: str,
    sample_order_events: list[dict[str, Any]],
    sample_customers_df: DataFrame,
    make_events_df: Callable[[list[dict[str, Any]]], DataFrame],
) -> None:
    """Events written to Bronze surface as aggregated metrics in Gold."""
    bronze = f"{tmp_delta_path}_bronze"
    silver = f"{tmp_delta_path}_silver"
    gold = f"{tmp_delta_path}_gold"
    ckpt_silver = f"{tmp_delta_path}_ckpt_silver"
    ckpt_gold = f"{tmp_delta_path}_ckpt_gold"

    # ---- Seed Bronze ----
    make_events_df(sample_order_events).write.format("delta").save(bronze)
    customers = sample_customers_df.drop("customer_state")

    # ---- Stage 1: Bronze → Silver (enrichment) ----
    bronze_stream = (
        spark.readStream.format("delta")
        .load(bronze)
        .withWatermark("event_ts", "10 minutes")
    )
    q1 = (
        enrich(bronze_stream, customers)
        .writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", ckpt_silver)
        .trigger(availableNow=True)
        .start(silver)
    )
    q1.awaitTermination()

    silver_rows = spark.read.format("delta").load(silver)
    assert silver_rows.count() == len(sample_order_events)
    assert "delivery_sla_tier" in silver_rows.columns

    # ---- Stage 2: Silver → Gold (aggregation) ----
    def _to_gold(batch_df: DataFrame, _batch_id: int) -> None:
        if not batch_df.rdd.isEmpty():
            upsert_delta(batch_df.sparkSession, batch_df, gold, _GOLD_MERGE)

    silver_stream = (
        spark.readStream.format("delta")
        .load(silver)
        .withWatermark("event_ts", "10 minutes")
    )
    q2 = (
        aggregate_state_category(silver_stream)
        .writeStream.foreachBatch(_to_gold)
        .outputMode("update")
        .option("checkpointLocation", ckpt_gold)
        .trigger(availableNow=True)
        .start()
    )
    q2.awaitTermination()

    # ---- Verify Gold ----
    gold_rows = spark.read.format("delta").load(gold)
    assert gold_rows.count() > 0

    totals = gold_rows.agg(
        F.sum("total_orders").alias("orders"),
        F.sum("total_revenue").alias("revenue"),
    ).collect()[0]
    assert totals["orders"] > 0
    assert totals["revenue"] > 0
    # Every Gold row carries the full metric set.
    for col in (
        "unique_customers",
        "high_value_orders",
        "cancellation_count",
        "avg_order_value",
    ):
        assert col in gold_rows.columns
