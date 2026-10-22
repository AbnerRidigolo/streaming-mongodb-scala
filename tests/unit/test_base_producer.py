"""Unit tests for the producers' environment-driven settings (no Kafka)."""

from __future__ import annotations

import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import fastavro
import pytest

import base_producer
from base_producer import encode_avro_json, kafka_config_from_env
from orders_producer import OrdersProducer

_SCHEMAS = Path(__file__).resolve().parents[2] / "producers" / "schemas"


def _order_record() -> dict:
    schema = json.loads((_SCHEMAS / "order_event.avsc").read_text(encoding="utf-8"))
    record = {
        "event_id": "e1",
        "event_type": "ORDER_CREATED",
        "order_id": "order_1",
        "customer_id": "cust_1",
        "seller_id": None,
        "payment_value": 99.9,
        "product_category": "esporte_lazer",
        "customer_state": "SP",
        "event_timestamp": 1_700_000_000_123,
        "metadata": {"source": "test", "version": "1.0", "producer_id": "pytest"},
    }
    for field in schema["fields"]:
        record.setdefault(field["name"], field.get("default"))
    return record


def test_kafka_cfg_variables_become_librdkafka_properties() -> None:
    """KAFKA_CFG_* maps to dotted lower-case keys; other variables are ignored."""
    env = {
        "KAFKA_CFG_SECURITY_PROTOCOL": "SASL_SSL",
        "KAFKA_CFG_SASL_MECHANISM": "PLAIN",
        "KAFKA_CFG_COMPRESSION_TYPE": "gzip",
        "KAFKA_CFG_REQUEST_TIMEOUT_MS": "60000",
        "KAFKA_BOOTSTRAP_SERVERS": "ignored:9092",
        "KAFKA_CFG_": "ignored",
    }

    assert kafka_config_from_env(env) == {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "compression.type": "gzip",
        "request.timeout.ms": "60000",
    }


def test_avro_json_encoding_round_trips_with_the_same_schema() -> None:
    """The JSON payload decodes back to the record with the .avsc schema."""
    schema = fastavro.parse_schema(
        json.loads((_SCHEMAS / "order_event.avsc").read_text(encoding="utf-8"))
    )
    record = _order_record()

    payload = encode_avro_json(schema, record)
    decoded = next(fastavro.json_reader(io.StringIO(payload.decode("utf-8")), schema))

    assert json.loads(payload)["payment_value"] == {"double": 99.9}
    assert decoded["order_id"] == "order_1"
    assert decoded["payment_value"] == 99.9
    assert decoded["seller_id"] is None


def test_avro_json_encoding_rejects_records_outside_the_contract() -> None:
    """A record missing a required field does not get published."""
    schema = fastavro.parse_schema(
        json.loads((_SCHEMAS / "order_event.avsc").read_text(encoding="utf-8"))
    )
    record = _order_record()
    del record["customer_state"]

    with pytest.raises(Exception):
        encode_avro_json(schema, record)


def test_json_mode_needs_no_schema_registry_and_applies_env_overrides(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """VALUE_FORMAT=json skips Schema Registry and sends Avro-JSON values."""
    for name in ("olist_customers_dataset.csv", "olist_order_items_dataset.csv"):
        (tmp_path / name).write_text(
            "customer_id,customer_state\n"
            if "customers" in name
            else "order_id,price,freight_value,seller_id,product_id\n",
            encoding="utf-8",
        )
    monkeypatch.setenv("VALUE_FORMAT", "json")
    monkeypatch.setenv("KAFKA_CFG_COMPRESSION_TYPE", "gzip")
    kafka_producer = MagicMock()

    with patch.object(
        base_producer, "Producer", return_value=kafka_producer
    ) as producer_cls, patch.object(base_producer, "SchemaRegistryClient") as registry:
        producer = OrdersProducer(
            "eh-namespace.servicebus.windows.net:9093",
            "http://unused:8081",
            "orders-raw",
            data_dir=tmp_path,
        )
        producer._publish(_order_record(), key="order_1")

    registry.assert_not_called()
    config = producer_cls.call_args.args[0]
    assert config["compression.type"] == "gzip"
    assert config["enable.idempotence"] is True
    sent = kafka_producer.produce.call_args.kwargs
    assert sent["topic"] == "orders-raw"
    assert json.loads(sent["value"])["order_id"] == "order_1"


def test_unknown_value_format_fails_fast(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A typo in VALUE_FORMAT stops the producer instead of sending garbage."""
    monkeypatch.setenv("VALUE_FORMAT", "protobuf")

    with patch.object(base_producer, "Producer"), pytest.raises(ValueError):
        OrdersProducer("x:9092", "http://unused:8081", "orders-raw", data_dir=tmp_path)
