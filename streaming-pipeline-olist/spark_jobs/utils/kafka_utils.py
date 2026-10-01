"""Kafka helper utilities: offsets, consumer lag, Avro decoding and topic admin.

These helpers wrap :mod:`confluent_kafka` so the streaming jobs, scripts and the
dashboard sidebar can introspect Kafka without re-implementing the boilerplate.
"""

from __future__ import annotations

from typing import Any

import structlog
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

log = structlog.get_logger("kafka_utils")

_OFFSET_TIMEOUT_SECONDS = 10.0


def get_latest_offsets(bootstrap_servers: str, topic: str) -> dict[int, int]:
    """Return the high-watermark (latest) offset for each partition of a topic.

    Args:
        bootstrap_servers: Kafka bootstrap servers (``host:port``).
        topic: Topic name to inspect.

    Returns:
        A mapping of ``partition_id -> latest_offset``. Empty if the topic does
        not exist.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": "offset-inspector",
            "enable.auto.commit": False,
        }
    )
    offsets: dict[int, int] = {}
    try:
        metadata = consumer.list_topics(topic, timeout=_OFFSET_TIMEOUT_SECONDS)
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            return offsets
        for partition_id in topic_meta.partitions:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition_id),
                timeout=_OFFSET_TIMEOUT_SECONDS,
            )
            offsets[partition_id] = high
    finally:
        consumer.close()
    return offsets


def get_consumer_lag(bootstrap_servers: str, topic: str, group_id: str) -> int:
    """Compute the total consumer lag of a group across all topic partitions.

    Lag is ``sum(high_watermark - committed_offset)`` over every partition. A
    partition with no committed offset contributes its full backlog.

    Args:
        bootstrap_servers: Kafka bootstrap servers.
        topic: Topic name.
        group_id: Consumer group whose lag is measured.

    Returns:
        Total lag in number of messages (``0`` if the topic is unknown).
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "enable.auto.commit": False,
        }
    )
    total_lag = 0
    try:
        metadata = consumer.list_topics(topic, timeout=_OFFSET_TIMEOUT_SECONDS)
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            return 0
        partitions = [TopicPartition(topic, pid) for pid in topic_meta.partitions]
        committed = consumer.committed(partitions, timeout=_OFFSET_TIMEOUT_SECONDS)
        committed_by_pid = {tp.partition: tp.offset for tp in committed}
        for pid in topic_meta.partitions:
            _, high = consumer.get_watermark_offsets(
                TopicPartition(topic, pid), timeout=_OFFSET_TIMEOUT_SECONDS
            )
            offset = committed_by_pid.get(pid, 0)
            # Offset is -1001 (OFFSET_INVALID) when nothing committed yet.
            if offset is None or offset < 0:
                offset = 0
            total_lag += max(0, high - offset)
    finally:
        consumer.close()
    return total_lag


def deserialize_avro_value(
    value_bytes: bytes,
    schema_registry_url: str,
    schema_str: str,
    topic: str = "",
) -> dict[str, Any]:
    """Decode a Confluent-Avro message value into a Python ``dict``.

    Args:
        value_bytes: Raw message value (Confluent wire format with magic byte).
        schema_registry_url: Schema Registry base URL.
        schema_str: Avro reader schema (``.avsc`` contents).
        topic: Topic name used to build the serialization context.

    Returns:
        The decoded record as a nested ``dict``.
    """
    client = SchemaRegistryClient({"url": schema_registry_url})
    deserializer = AvroDeserializer(client, schema_str)
    ctx = SerializationContext(topic, MessageField.VALUE)
    return deserializer(value_bytes, ctx)


def create_topic(
    bootstrap_servers: str,
    topic_name: str,
    partitions: int,
    replication_factor: int,
    config: dict[str, str] | None = None,
) -> bool:
    """Create a Kafka topic if it does not already exist (idempotent).

    Args:
        bootstrap_servers: Kafka bootstrap servers.
        topic_name: Name of the topic to create.
        partitions: Number of partitions.
        replication_factor: Replication factor.
        config: Optional topic-level configuration overrides.

    Returns:
        ``True`` if the topic was created, ``False`` if it already existed.
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=_OFFSET_TIMEOUT_SECONDS).topics
    if topic_name in existing:
        log.info("topic.exists", topic=topic_name)
        return False

    new_topic = NewTopic(
        topic_name,
        num_partitions=partitions,
        replication_factor=replication_factor,
        config=config or {},
    )
    futures = admin.create_topics([new_topic])
    futures[topic_name].result()  # Raises on failure.
    log.info(
        "topic.created",
        topic=topic_name,
        partitions=partitions,
        replication_factor=replication_factor,
        config=config or {},
    )
    return True
