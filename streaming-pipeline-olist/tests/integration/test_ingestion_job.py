"""Integration tests for the ingestion job.

The transformation/idempotency logic is tested directly against Spark. A full
Kafka round-trip test is included but skipped unless ``RUN_KAFKA_IT=1`` and a
broker is reachable (``make up`` first).
"""

from __future__ import annotations

import json
import os
import uuid

import pytest
from pyspark.sql import SparkSession

from ingestion_job import (
    ORDER_EVENT_SCHEMA,
    _add_audit_columns,
    _parse_kafka_value,
)
from utils.delta_utils import upsert_delta


def _event_json(event_id: str, ts_ms: int) -> str:
    """Serialize a complete OrderEvent as a JSON string."""
    return json.dumps(
        {
            "event_id": event_id,
            "event_type": "ORDER_CREATED",
            "order_id": "order_1",
            "customer_id": "cust_1",
            "seller_id": "seller_1",
            "payment_value": 199.9,
            "product_category": "beleza_saude",
            "customer_state": "SP",
            "event_timestamp": ts_ms,
            "metadata": {"source": "test", "version": "1.0", "producer_id": "pytest"},
        }
    )


def test_parse_kafka_value_json_decodes_all_fields(spark: SparkSession) -> None:
    """The JSON parse path decodes every OrderEvent field."""
    raw = spark.createDataFrame(
        [(_event_json("e1", 1_700_000_000_000),)], schema=["value"]
    )
    parsed = _parse_kafka_value(raw, "json")
    row = parsed.collect()[0]

    assert set(f.name for f in ORDER_EVENT_SCHEMA.fields).issubset(set(parsed.columns))
    assert row["event_id"] == "e1"
    assert row["customer_state"] == "SP"
    assert row["payment_value"] == 199.9
    assert row["metadata"]["producer_id"] == "pytest"


def test_add_audit_columns_adds_partition_columns(spark: SparkSession) -> None:
    """Audit columns derive event_ts plus the partition columns."""
    raw = spark.createDataFrame(
        [(_event_json("e1", 1_700_000_000_000),)], schema=["value"]
    )
    parsed = _parse_kafka_value(raw, "json")
    audited = _add_audit_columns(parsed)

    for col in ("event_ts", "_ingested_at", "_date", "_hour"):
        assert col in audited.columns
    row = audited.collect()[0]
    assert row["_hour"] is not None
    assert row["_date"] is not None


def test_merge_dedups_by_event_id(spark: SparkSession, tmp_delta_path: str) -> None:
    """Duplicate event_ids collapse to a single Bronze row and re-runs are stable."""
    dup_id = str(uuid.uuid4())
    raw = spark.createDataFrame(
        [
            (_event_json(dup_id, 1_700_000_000_000),),
            (_event_json(dup_id, 1_700_000_000_000),),
            (_event_json(str(uuid.uuid4()), 1_700_000_000_000),),
        ],
        schema=["value"],
    )
    events = _add_audit_columns(_parse_kafka_value(raw, "json"))
    deduped = events.dropDuplicates(["event_id"])

    upsert_delta(spark, deduped, tmp_delta_path, "t.event_id = s.event_id")
    first = spark.read.format("delta").load(tmp_delta_path).count()
    upsert_delta(spark, deduped, tmp_delta_path, "t.event_id = s.event_id")
    second = spark.read.format("delta").load(tmp_delta_path).count()

    assert first == 2
    assert second == 2


@pytest.mark.skipif(
    os.environ.get("RUN_KAFKA_IT") != "1",
    reason="Set RUN_KAFKA_IT=1 with a running broker to exercise the Kafka path.",
)
def test_kafka_topic_reachable() -> None:
    """Smoke test: the orders topic exists on the configured broker."""
    from confluent_kafka.admin import AdminClient

    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic = os.environ.get("TOPIC_ORDERS_RAW", "orders-raw")
    admin = AdminClient({"bootstrap.servers": bootstrap})
    topics = admin.list_topics(timeout=10).topics
    assert topic in topics
