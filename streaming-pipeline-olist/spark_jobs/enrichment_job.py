"""Enrichment job: Bronze Delta → Silver Delta (watermark + stream-static join).

Reads the Bronze order events as a stream, applies an event-time watermark, and
joins them against a *static, broadcast* customers dimension (a classic
stream-static join). It then derives business attributes — delivery SLA tier,
revenue bucket, high-value flag, day/hour — and appends the result to the Silver
Delta table partitioned by date and customer state.
"""

from __future__ import annotations

import os

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = structlog.get_logger("enrichment_job")

WATERMARK_DELAY = "10 minutes"


def build_customers_dim(spark: SparkSession, customers_path: str) -> DataFrame:
    """Load and de-duplicate the static customers dimension.

    Args:
        spark: Active SparkSession.
        customers_path: Path to the customers parquet dataset.

    Returns:
        A DataFrame with one row per ``customer_id``.
    """
    cols = [
        "customer_id",
        "customer_unique_id",
        "customer_zip_code_prefix",
        "customer_city",
    ]
    dim = spark.read.parquet(customers_path)
    present = [c for c in cols if c in dim.columns]
    return dim.select(*present).dropDuplicates(["customer_id"])


def enrich(events: DataFrame, customers: DataFrame) -> DataFrame:
    """Join events with the customers dimension and derive business columns.

    Args:
        events: Stream (or batch) of Bronze order events. Must contain
            ``customer_id``, ``customer_state``, ``payment_value`` and
            ``event_ts``.
        customers: Static customers dimension keyed by ``customer_id``.

    Returns:
        The enriched DataFrame with the derived attribute columns added.
    """
    customers_dedup = customers.dropDuplicates(["customer_id"])
    joined = events.join(
        F.broadcast(customers_dedup), on="customer_id", how="left"
    )

    payment = F.coalesce(F.col("payment_value"), F.lit(0.0))
    return (
        joined.withColumn(
            "delivery_sla_tier",
            F.when(F.col("customer_state") == "SP", F.lit("D+3"))
            .when(F.col("customer_state").isin("RJ", "MG", "ES"), F.lit("D+5"))
            .otherwise(F.lit("D+8")),
        )
        .withColumn(
            "revenue_bucket",
            F.when(payment < 50, F.lit("low"))
            .when(payment < 200, F.lit("mid"))
            .otherwise(F.lit("high")),
        )
        .withColumn("is_high_value", payment > 500)
        .withColumn("day_of_week", F.dayofweek(F.col("event_ts")))
        .withColumn("hour_of_day", F.hour(F.col("event_ts")))
    )


def run_enrichment_job(spark: SparkSession) -> "StreamingQuery":  # noqa: F821
    """Start the Bronze → Silver enrichment streaming query.

    Args:
        spark: Active SparkSession.

    Returns:
        The started :class:`pyspark.sql.streaming.StreamingQuery`.
    """
    bronze_path = os.environ.get("BRONZE_PATH", "data/bronze/orders")
    silver_path = os.environ.get("SILVER_PATH", "data/silver/orders_enriched")
    customers_path = os.environ.get("CUSTOMERS_PATH", "data/reference/customers")
    checkpoint = os.environ.get("CHECKPOINT_LOCATION", "/tmp/streaming-checkpoints")

    customers = build_customers_dim(spark, customers_path)

    stream = (
        spark.readStream.format("delta")
        .option("maxFilesPerTrigger", "10")
        .load(bronze_path)
        .withWatermark("event_ts", WATERMARK_DELAY)
    )

    enriched = enrich(stream, customers)

    query = (
        enriched.writeStream.format("delta")
        .outputMode("append")
        .partitionBy("_date", "customer_state")
        .option("checkpointLocation", f"{checkpoint}/enrichment")
        .trigger(processingTime="5 seconds")
        .queryName("enrichment-silver")
        .start(silver_path)
    )
    log.info(
        "enrichment.started",
        bronze_path=bronze_path,
        silver_path=silver_path,
        watermark=WATERMARK_DELAY,
    )
    return query


def main() -> None:
    """Standalone entry point: run only the enrichment job until terminated."""
    from spark_session import get_streaming_session

    spark = get_streaming_session(
        "olist-enrichment", os.environ.get("SPARK_ENV", "local")
    )
    query = run_enrichment_job(spark)
    query.awaitTermination()


if __name__ == "__main__":
    main()
