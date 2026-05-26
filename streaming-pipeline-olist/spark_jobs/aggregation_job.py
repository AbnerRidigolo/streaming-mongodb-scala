"""Aggregation job: Silver Delta → Gold Delta (windowed metrics + merge).

Runs two windowed aggregations over the watermarked Silver stream:

* **Agg 1** — tumbling-ish 1-minute window sliding every 30s, grouped by
  ``(customer_state, product_category)``: order counts, revenue, ticket,
  unique customers, high-value and cancellation counts. Written to the Gold
  *windows* table.
* **Agg 2** — 5-minute window sliding every minute, global: orders-per-minute
  and revenue rate, plus the top state/category in each window. Written to the
  Gold *rate* table.

Both sinks use ``foreachBatch`` + Delta MERGE keyed by the window bounds so the
``update`` output mode is idempotent across restarts.
"""

from __future__ import annotations

import os

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from utils.delta_utils import upsert_delta

log = structlog.get_logger("aggregation_job")

WATERMARK_DELAY = "10 minutes"


def aggregate_state_category(df: DataFrame) -> DataFrame:
    """Aggregate orders into a 1-min/30s window by state and category.

    Args:
        df: Silver events (stream or batch) with ``event_ts``, ``payment_value``,
            ``customer_id``, ``customer_state``, ``product_category``,
            ``is_high_value`` and ``event_type``.

    Returns:
        A windowed DataFrame with ``window_start``/``window_end`` plus the
        per-group metrics.
    """
    category = F.coalesce(F.col("product_category"), F.lit("unknown"))
    return (
        df.withColumn("product_category", category)
        .groupBy(
            F.window(F.col("event_ts"), "1 minute", "30 seconds"),
            F.col("customer_state"),
            F.col("product_category"),
        )
        .agg(
            F.count("*").alias("total_orders"),
            F.round(F.sum("payment_value"), 2).alias("total_revenue"),
            F.round(F.avg("payment_value"), 2).alias("avg_order_value"),
            F.approx_count_distinct("customer_id").alias("unique_customers"),
            F.sum(F.when(F.col("is_high_value"), 1).otherwise(0)).alias(
                "high_value_orders"
            ),
            F.sum(
                F.when(F.col("event_type") == "ORDER_CANCELED", 1).otherwise(0)
            ).alias("cancellation_count"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("customer_state"),
            F.col("product_category"),
            "total_orders",
            "total_revenue",
            "avg_order_value",
            "unique_customers",
            "high_value_orders",
            "cancellation_count",
        )
    )


def aggregate_global_base(df: DataFrame) -> DataFrame:
    """Aggregate orders into a 5-min/1min window by state and category.

    This is the *base* for the global rate metrics — it is collapsed to a single
    row per window inside :func:`finalize_global_rate`.

    Args:
        df: Silver events (stream or batch).

    Returns:
        A windowed DataFrame grouped by window, state and category.
    """
    category = F.coalesce(F.col("product_category"), F.lit("unknown"))
    return (
        df.withColumn("product_category", category)
        .groupBy(
            F.window(F.col("event_ts"), "5 minutes", "1 minute"),
            F.col("customer_state"),
            F.col("product_category"),
        )
        .agg(
            F.count("*").alias("order_count"),
            F.round(F.sum("payment_value"), 2).alias("revenue"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "customer_state",
            "product_category",
            "order_count",
            "revenue",
        )
    )


def finalize_global_rate(base: DataFrame) -> DataFrame:
    """Collapse the global base into one row/window with rate + top dimensions.

    Args:
        base: Output of :func:`aggregate_global_base` (a *static* batch).

    Returns:
        One row per window with ``orders_per_minute``, ``revenue_rate``,
        ``top_state`` and ``top_category``.
    """
    window_key = Window.partitionBy("window_start", "window_end")

    totals = base.groupBy("window_start", "window_end").agg(
        F.sum("order_count").alias("total_orders"),
        F.round(F.sum("revenue"), 2).alias("total_revenue"),
    )

    state_rank = (
        base.groupBy("window_start", "window_end", "customer_state")
        .agg(F.sum("order_count").alias("state_orders"))
        .withColumn(
            "rnk",
            F.row_number().over(window_key.orderBy(F.desc("state_orders"))),
        )
        .where(F.col("rnk") == 1)
        .select("window_start", "window_end", F.col("customer_state").alias("top_state"))
    )

    category_rank = (
        base.groupBy("window_start", "window_end", "product_category")
        .agg(F.sum("order_count").alias("cat_orders"))
        .withColumn(
            "rnk",
            F.row_number().over(window_key.orderBy(F.desc("cat_orders"))),
        )
        .where(F.col("rnk") == 1)
        .select(
            "window_start",
            "window_end",
            F.col("product_category").alias("top_category"),
        )
    )

    return (
        totals.withColumn(
            "orders_per_minute", F.round(F.col("total_orders") / 5.0, 2)
        )
        .withColumn("revenue_rate", F.round(F.col("total_revenue") / 5.0, 2))
        .join(state_rank, ["window_start", "window_end"], "left")
        .join(category_rank, ["window_start", "window_end"], "left")
        .select(
            "window_start",
            "window_end",
            "orders_per_minute",
            "revenue_rate",
            "top_state",
            "top_category",
        )
    )


def run_aggregation_job(spark: SparkSession) -> list["StreamingQuery"]:  # noqa: F821
    """Start both Gold aggregation streaming queries.

    Args:
        spark: Active SparkSession.

    Returns:
        The two started :class:`pyspark.sql.streaming.StreamingQuery` objects
        (state/category windows, then the global rate).
    """
    silver_path = os.environ.get("SILVER_PATH", "data/silver/orders_enriched")
    gold_path = os.environ.get("GOLD_PATH", "data/gold/orders_agg")
    gold_rate_path = os.environ.get("GOLD_RATE_PATH", f"{gold_path}_rate")
    checkpoint = os.environ.get("CHECKPOINT_LOCATION", "/tmp/streaming-checkpoints")

    silver = (
        spark.readStream.format("delta")
        .load(silver_path)
        .withWatermark("event_ts", WATERMARK_DELAY)
    )

    # ---- Query A: 1-min/30s state x category windows ----
    def _write_windows(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        prepared = batch_df.withColumn(
            "window_date", F.to_date("window_start")
        )
        stats = upsert_delta(
            spark=batch_df.sparkSession,
            df=prepared,
            path=gold_path,
            merge_condition=(
                "t.window_start = s.window_start AND "
                "t.window_end = s.window_end AND "
                "t.customer_state = s.customer_state AND "
                "t.product_category = s.product_category"
            ),
            partition_cols=["window_date"],
        )
        log.info("aggregation.windows.committed", batch_id=batch_id, **stats)

    query_windows = (
        aggregate_state_category(silver)
        .writeStream.foreachBatch(_write_windows)
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint}/aggregation")
        .trigger(processingTime="10 seconds")
        .queryName("aggregation-gold-windows")
        .start()
    )

    # ---- Query B: 5-min/1min global rate ----
    def _write_rate(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return
        finalized = finalize_global_rate(batch_df)
        stats = upsert_delta(
            spark=batch_df.sparkSession,
            df=finalized,
            path=gold_rate_path,
            merge_condition=(
                "t.window_start = s.window_start AND "
                "t.window_end = s.window_end"
            ),
            partition_cols=None,
        )
        log.info("aggregation.rate.committed", batch_id=batch_id, **stats)

    query_rate = (
        aggregate_global_base(silver)
        .writeStream.foreachBatch(_write_rate)
        .outputMode("update")
        .option("checkpointLocation", f"{checkpoint}/aggregation_rate")
        .trigger(processingTime="10 seconds")
        .queryName("aggregation-gold-rate")
        .start()
    )

    log.info(
        "aggregation.started",
        silver_path=silver_path,
        gold_path=gold_path,
        gold_rate_path=gold_rate_path,
    )
    return [query_windows, query_rate]


def main() -> None:
    """Standalone entry point: run only the aggregation queries."""
    from spark_session import get_streaming_session

    spark = get_streaming_session(
        "olist-aggregation", os.environ.get("SPARK_ENV", "local")
    )
    queries = run_aggregation_job(spark)
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
