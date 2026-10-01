"""Ingestion job: Kafka ``orders-raw`` → Bronze Delta (exactly-once).

Reads the raw order events from Kafka, decodes the Confluent-Avro (or JSON)
payload using the full ``OrderEvent`` schema, adds audit columns and MERGEs the
records into the Bronze Delta table keyed by ``event_id``. The MERGE makes the
write idempotent: replaying the same offsets never duplicates rows, which —
combined with Structured Streaming checkpointing — yields exactly-once delivery
into the lakehouse.
"""

from __future__ import annotations

import os
from typing import Final

import structlog
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import types as T

from utils.delta_utils import upsert_delta

log = structlog.get_logger("ingestion_job")

# --- Spark schema mirroring producers/schemas/order_event.avsc ---------------
ORDER_EVENT_SCHEMA: Final[T.StructType] = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("event_type", T.StringType(), False),
        T.StructField("order_id", T.StringType(), False),
        T.StructField("customer_id", T.StringType(), False),
        T.StructField("seller_id", T.StringType(), True),
        T.StructField("payment_value", T.DoubleType(), True),
        T.StructField("product_category", T.StringType(), True),
        T.StructField("customer_state", T.StringType(), False),
        T.StructField("event_timestamp", T.LongType(), False),
        T.StructField(
            "metadata",
            T.StructType(
                [
                    T.StructField("source", T.StringType(), True),
                    T.StructField("version", T.StringType(), True),
                    T.StructField("producer_id", T.StringType(), True),
                ]
            ),
            True,
        ),
    ]
)

# --- Avro reader schema (Confluent wire format) ------------------------------
ORDER_EVENT_AVRO: Final[str] = """
{
  "type": "record", "name": "OrderEvent", "namespace": "br.com.olist.events",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "event_type", "type": {"type": "enum", "name": "OrderEventType",
      "symbols": ["ORDER_CREATED","ORDER_APPROVED","ORDER_SHIPPED",
                  "ORDER_DELIVERED","ORDER_CANCELED"]}},
    {"name": "order_id", "type": "string"},
    {"name": "customer_id", "type": "string"},
    {"name": "seller_id", "type": ["null","string"], "default": null},
    {"name": "payment_value", "type": ["null","double"], "default": null},
    {"name": "product_category", "type": ["null","string"], "default": null},
    {"name": "customer_state", "type": "string"},
    {"name": "event_timestamp", "type": {"type": "long",
      "logicalType": "timestamp-millis"}},
    {"name": "metadata", "type": {"type": "record", "name": "EventMetadata",
      "fields": [
        {"name": "source", "type": "string"},
        {"name": "version", "type": "string", "default": "1.0"},
        {"name": "producer_id", "type": "string"}
      ]}}
  ]
}
"""


def _parse_kafka_value(raw: DataFrame, value_format: str) -> DataFrame:
    """Decode the Kafka ``value`` column into the OrderEvent columns.

    Args:
        raw: The raw Kafka source DataFrame (``key``, ``value`` binary, ...).
        value_format: ``"avro"`` (Confluent wire format) or ``"json"``.

    Returns:
        A DataFrame with the flattened ``OrderEvent`` columns.
    """
    if value_format == "avro":
        from pyspark.sql.avro.functions import from_avro

        # Strip the 5-byte Confluent header (magic byte + 4-byte schema id).
        stripped = F.expr("substring(value, 6, length(value) - 5)")
        decoded = raw.select(
            from_avro(stripped, ORDER_EVENT_AVRO).alias("data")
        )
    else:
        decoded = raw.select(
            F.from_json(F.col("value").cast("string"), ORDER_EVENT_SCHEMA).alias(
                "data"
            )
        )
    return decoded.select("data.*").where(F.col("event_id").isNotNull())


def _add_audit_columns(df: DataFrame) -> DataFrame:
    """Add ``event_ts`` plus the partition/audit columns to the events.

    Args:
        df: Parsed OrderEvent DataFrame.

    Returns:
        The DataFrame enriched with ``event_ts``, ``_ingested_at``, ``_date``
        and ``_hour``.
    """
    return (
        df.withColumn(
            "event_ts", (F.col("event_timestamp") / 1000).cast("timestamp")
        )
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_date", F.to_date(F.col("event_ts")))
        .withColumn("_hour", F.hour(F.col("event_ts")))
    )


def run_ingestion_job(spark: SparkSession) -> StreamingQuery:
    """Start the Kafka → Bronze ingestion streaming query.

    Args:
        spark: Active SparkSession (configured with Kafka + Delta).

    Returns:
        The started :class:`pyspark.sql.streaming.StreamingQuery`.
    """
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("TOPIC_ORDERS_RAW", "orders-raw")
    bronze_path = os.environ.get("BRONZE_PATH", "data/bronze/orders")
    checkpoint = os.environ.get("CHECKPOINT_LOCATION", "/tmp/streaming-checkpoints")
    value_format = os.environ.get("KAFKA_VALUE_FORMAT", "avro").lower()

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("maxOffsetsPerTrigger", "10000")
        .option("failOnDataLoss", "false")
        .option("kafka.group.id", "spark-ingestion-job")
        .load()
    )

    events = _add_audit_columns(_parse_kafka_value(raw, value_format))

    def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
        """foreachBatch sink: MERGE the micro-batch into Bronze by event_id."""
        import time

        start = time.monotonic()
        records_read = batch_df.count()
        if records_read == 0:
            log.debug("ingestion.batch.empty", batch_id=batch_id)
            return
        deduped = batch_df.dropDuplicates(["event_id"])
        stats = upsert_delta(
            spark=batch_df.sparkSession,
            df=deduped,
            path=bronze_path,
            merge_condition="t.event_id = s.event_id",
            partition_cols=["_date", "event_type", "_hour"],
        )
        duration_ms = int((time.monotonic() - start) * 1000)
        log.info(
            "ingestion.batch.committed",
            batch_id=batch_id,
            records_read=records_read,
            records_written=stats["rows_inserted"] + stats["rows_updated"],
            duration_ms=duration_ms,
        )

    query = (
        events.writeStream.foreachBatch(_write_batch)
        .option("checkpointLocation", f"{checkpoint}/ingestion")
        .trigger(processingTime="2 seconds")
        .queryName("ingestion-bronze")
        .start()
    )
    log.info(
        "ingestion.started",
        topic=topic,
        bronze_path=bronze_path,
        value_format=value_format,
    )
    return query


def main() -> None:
    """Standalone entry point: run only the ingestion job until terminated."""
    from spark_session import get_streaming_session

    spark = get_streaming_session(
        "olist-ingestion", os.environ.get("SPARK_ENV", "local")
    )
    query = run_ingestion_job(spark)
    query.awaitTermination()


if __name__ == "__main__":
    main()
