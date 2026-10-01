"""Unit tests for :mod:`spark_jobs.utils.kafka_utils` (clients mocked)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from confluent_kafka import TopicPartition

from utils import kafka_utils


class _FakeMeta:
    """Minimal stand-in for confluent_kafka topic metadata."""

    def __init__(self, partitions: dict[int, None], error: object = None) -> None:
        self.partitions = partitions
        self.error = error


class _FakeListTopics:
    """Minimal stand-in for the ``list_topics`` result."""

    def __init__(self, topics: dict) -> None:
        self.topics = topics


def test_get_latest_offsets_returns_high_watermarks() -> None:
    """Latest offsets equal the high watermark per partition."""
    consumer = MagicMock()
    consumer.list_topics.return_value = _FakeListTopics(
        {"orders-raw": _FakeMeta({0: None, 1: None})}
    )
    consumer.get_watermark_offsets.return_value = (0, 100)

    with patch.object(kafka_utils, "Consumer", return_value=consumer):
        offsets = kafka_utils.get_latest_offsets("kafka:9092", "orders-raw")

    assert offsets == {0: 100, 1: 100}
    consumer.close.assert_called_once()


def test_get_consumer_lag_sums_partition_lag() -> None:
    """Total lag is the sum of (high - committed) across partitions."""
    consumer = MagicMock()
    consumer.list_topics.return_value = _FakeListTopics(
        {"orders-raw": _FakeMeta({0: None, 1: None})}
    )
    consumer.get_watermark_offsets.return_value = (0, 100)
    consumer.committed.return_value = [
        TopicPartition("orders-raw", 0, 40),
        TopicPartition("orders-raw", 1, 30),
    ]

    with patch.object(kafka_utils, "Consumer", return_value=consumer):
        lag = kafka_utils.get_consumer_lag("kafka:9092", "orders-raw", "g1")

    assert lag == (100 - 40) + (100 - 30)


def test_create_topic_skips_when_exists() -> None:
    """An already-present topic is not recreated."""
    admin = MagicMock()
    admin.list_topics.return_value = _FakeListTopics({"orders-raw": _FakeMeta({})})

    with patch.object(kafka_utils, "AdminClient", return_value=admin):
        created = kafka_utils.create_topic("kafka:9092", "orders-raw", 3, 1)

    assert created is False
    admin.create_topics.assert_not_called()


def test_create_topic_creates_when_absent() -> None:
    """A missing topic is created and the future is awaited."""
    admin = MagicMock()
    admin.list_topics.return_value = _FakeListTopics({})
    future = MagicMock()
    future.result.return_value = None
    admin.create_topics.return_value = {"new-topic": future}

    with patch.object(kafka_utils, "AdminClient", return_value=admin):
        created = kafka_utils.create_topic(
            "kafka:9092", "new-topic", 6, 1, {"cleanup.policy": "compact"}
        )

    assert created is True
    admin.create_topics.assert_called_once()
    future.result.assert_called_once()


def test_deserialize_avro_value_decodes_to_dict() -> None:
    """Avro deserialization delegates to the Schema Registry deserializer."""
    deserializer = MagicMock(return_value={"event_id": "abc", "value": 1})

    with patch.object(kafka_utils, "SchemaRegistryClient"), patch.object(
        kafka_utils, "AvroDeserializer", return_value=deserializer
    ):
        result = kafka_utils.deserialize_avro_value(
            b"\x00\x00\x00\x00\x01payload", "http://sr:8081", "{}", "orders-raw"
        )

    assert result == {"event_id": "abc", "value": 1}
    deserializer.assert_called_once()
