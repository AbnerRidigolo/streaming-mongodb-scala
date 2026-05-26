"""Base Kafka producer with Avro serialization, retries and Prometheus metrics.

This module provides :class:`BaseProducer`, an abstract base class that
encapsulates all the boilerplate required to publish strongly-typed events to
Kafka using Confluent Schema Registry + Avro:

* idempotent, durable producer configuration (``acks=all`` + idempotence),
* automatic Avro serialization driven by a registered ``.avsc`` schema,
* a structured-logging delivery callback,
* Prometheus counters for produced messages,
* a CSV replay loop suitable for continuous demos.

Concrete producers subclass :class:`BaseProducer` and implement
:meth:`BaseProducer._build_event`.
"""

from __future__ import annotations

import abc
import dataclasses
import logging
import os
import time
from pathlib import Path
from typing import Any, Iterator

import structlog
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    MessageField,
    SerializationContext,
    StringSerializer,
)
from prometheus_client import Counter, start_http_server

# ---------------------------------------------------------------------------
# Structured logging configuration (shared by every producer)
# ---------------------------------------------------------------------------


def configure_logging(level: str = "INFO") -> None:
    """Configure :mod:`structlog` for JSON-friendly structured output.

    Args:
        level: Root logging level name (e.g. ``"INFO"``, ``"WARNING"``).
    """
    logging.basicConfig(
        format="%(message)s",
        level=getattr(logging, level.upper(), logging.INFO),
    )
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


# ---------------------------------------------------------------------------
# Prometheus metrics (module-level singletons)
# ---------------------------------------------------------------------------

MESSAGES_PRODUCED_TOTAL = Counter(
    "kafka_messages_produced_total",
    "Total number of Kafka messages produced by this producer.",
    labelnames=("topic", "status"),
)

_METRICS_SERVER_STARTED = False


def start_metrics_server(port: int) -> None:
    """Start the Prometheus metrics HTTP server exactly once per process.

    Args:
        port: TCP port on which to expose ``/metrics``.
    """
    global _METRICS_SERVER_STARTED
    if _METRICS_SERVER_STARTED:
        return
    try:
        start_http_server(port)
        _METRICS_SERVER_STARTED = True
    except OSError:
        # Port already bound (e.g. another producer in the same host) — ignore.
        _METRICS_SERVER_STARTED = True


# ---------------------------------------------------------------------------
# Shared event metadata
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class EventMetadata:
    """Common envelope metadata attached to every produced event.

    Attributes:
        source: Logical name of the producing component.
        producer_id: Unique identifier of the producer instance.
        version: Schema/contract version string.
    """

    source: str
    producer_id: str
    version: str = "1.0"


# ---------------------------------------------------------------------------
# Base producer
# ---------------------------------------------------------------------------


class BaseProducer(abc.ABC):
    """Abstract Avro Kafka producer with idempotent delivery and metrics.

    Subclasses must implement :meth:`_build_event`, which converts a raw input
    record (a ``dict`` row) into a serializable :func:`dataclasses.dataclass`
    instance matching the Avro schema registered for ``topic``.

    Attributes:
        topic: Destination Kafka topic.
        events_per_second: Target publish rate used to throttle the CSV replay.
        log: Bound structlog logger.
    """

    #: Default producer tuning applied to every concrete producer.
    PRODUCER_CONFIG: dict[str, Any] = {
        "acks": "all",
        "enable.idempotence": True,
        "compression.type": "lz4",
        "linger.ms": 50,
        "batch.size": 65536,
        "retries": 5,
        "retry.backoff.ms": 200,
    }

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        events_per_second: int = 10,
        *,
        schema_path: str | os.PathLike[str] | None = None,
        schema_str: str | None = None,
        producer_id: str | None = None,
        source: str | None = None,
    ) -> None:
        """Initialize the producer, Schema Registry client and Avro serializer.

        Args:
            bootstrap_servers: Kafka bootstrap servers (``host:port``).
            schema_registry_url: Confluent Schema Registry base URL.
            topic: Destination topic for produced events.
            events_per_second: Target throughput used to pace the CSV replay.
            schema_path: Optional path to the ``.avsc`` schema. Defaults to the
                value returned by :meth:`_default_schema_path`.
            schema_str: Optional inline Avro schema string. Takes precedence over
                ``schema_path`` when provided (used by producers without a
                dedicated ``.avsc`` file on disk).
            producer_id: Identifier embedded in event metadata. Defaults to the
                ``PRODUCER_ID`` environment variable.
            source: Logical source name embedded in event metadata. Defaults to
                the concrete class name.
        """
        self.topic = topic
        self.events_per_second = max(1, int(events_per_second))
        self.producer_id = producer_id or os.environ.get(
            "PRODUCER_ID", "producer-001"
        )
        self.source = source or self.__class__.__name__
        self.log = structlog.get_logger(self.__class__.__name__).bind(topic=topic)

        # Avro schema --------------------------------------------------------
        if schema_str is not None:
            self._schema_str = schema_str
            schema_name = "<inline>"
        else:
            schema_file = Path(schema_path or self._default_schema_path())
            self._schema_str = schema_file.read_text(encoding="utf-8")
            schema_name = schema_file.name

        # Schema Registry + serializers -------------------------------------
        self._sr_client = SchemaRegistryClient({"url": schema_registry_url})
        self._avro_serializer = AvroSerializer(
            self._sr_client,
            self._schema_str,
            to_dict=lambda obj, ctx: self._event_to_dict(obj),
            conf={"auto.register.schemas": True},
        )
        self._key_serializer = StringSerializer("utf_8")

        # Kafka producer -----------------------------------------------------
        config = {"bootstrap.servers": bootstrap_servers, **self.PRODUCER_CONFIG}
        self._producer = Producer(config)

        self._produced = 0
        self.log.info(
            "producer.initialized",
            bootstrap_servers=bootstrap_servers,
            schema_registry_url=schema_registry_url,
            events_per_second=self.events_per_second,
            schema=schema_name,
        )

    # ------------------------------------------------------------------ hooks

    @abc.abstractmethod
    def _build_event(self, row: dict[str, Any]) -> Any:
        """Build a serializable event dataclass from a raw input row.

        Args:
            row: A single record (e.g. a CSV row as a ``dict``).

        Returns:
            A :func:`dataclasses.dataclass` instance matching the Avro schema.
        """
        raise NotImplementedError

    @abc.abstractmethod
    def _default_schema_path(self) -> Path:
        """Return the default ``.avsc`` schema path for this producer."""
        raise NotImplementedError

    def _iter_rows(self, csv_path: str | os.PathLike[str]) -> Iterator[dict[str, Any]]:
        """Yield input rows to be turned into events.

        The default implementation streams a CSV file row-by-row. Subclasses
        that need joins or richer preprocessing should override this.

        Args:
            csv_path: Path to the CSV file to replay.

        Yields:
            One ``dict`` per logical input record.
        """
        import csv

        with open(csv_path, newline="", encoding="utf-8") as handle:
            yield from csv.DictReader(handle)

    # ----------------------------------------------------------- serialization

    @staticmethod
    def _event_to_dict(event: Any) -> dict[str, Any]:
        """Convert an event dataclass into a plain ``dict`` for Avro encoding.

        Args:
            event: A dataclass instance (possibly with nested dataclasses).

        Returns:
            A nested ``dict`` representation suitable for the Avro serializer.
        """
        if dataclasses.is_dataclass(event) and not isinstance(event, type):
            return dataclasses.asdict(event)
        if isinstance(event, dict):
            return event
        raise TypeError(f"Cannot serialize event of type {type(event)!r}")

    # -------------------------------------------------------------- callbacks

    def _delivery_report(self, err: Any, msg: Any) -> None:
        """Delivery callback invoked by ``poll``/``flush`` per message.

        Args:
            err: A ``KafkaError`` if delivery failed, otherwise ``None``.
            msg: The delivered (or failed) ``Message`` handle.
        """
        if err is not None:
            MESSAGES_PRODUCED_TOTAL.labels(topic=self.topic, status="error").inc()
            self.log.error(
                "delivery.failed",
                error=str(err),
                key=msg.key().decode("utf-8") if msg.key() else None,
            )
            return

        MESSAGES_PRODUCED_TOTAL.labels(topic=self.topic, status="success").inc()
        self.log.debug(
            "delivery.success",
            partition=msg.partition(),
            offset=msg.offset(),
        )

    # ------------------------------------------------------------------ publish

    def _publish(self, event: Any, key: str | None = None) -> None:
        """Serialize an event to Avro and enqueue it for delivery.

        Args:
            event: The event dataclass instance to publish.
            key: Optional partition key (e.g. ``order_id``).
        """
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        value_bytes = self._avro_serializer(event, ctx)
        self._producer.produce(
            topic=self.topic,
            key=self._key_serializer(key) if key is not None else None,
            value=value_bytes,
            on_delivery=self._delivery_report,
        )
        # Serve delivery callbacks without blocking.
        self._producer.poll(0)
        self._produced += 1

    # --------------------------------------------------------------- replay loop

    def produce_from_csv(
        self,
        csv_path: str | os.PathLike[str],
        loop: bool = False,
    ) -> None:
        """Replay a CSV file as a stream of events at the configured rate.

        Args:
            csv_path: Path to the source CSV file.
            loop: When ``True``, restart from the top after reaching EOF —
                useful for continuous demos / screen recordings.
        """
        start_metrics_server(int(os.environ.get("PRODUCER_METRICS_PORT", "8000")))
        delay = 1.0 / self.events_per_second
        pass_no = 0
        try:
            while True:
                pass_no += 1
                self.log.info("replay.pass.start", pass_no=pass_no, csv=str(csv_path))
                for processed, row in enumerate(self._iter_rows(csv_path), start=1):
                    for event, key in self._events_for_row(row):
                        self._publish(event, key=key)
                        time.sleep(delay)
                    if processed % 500 == 0:
                        self.log.info(
                            "replay.progress",
                            rows_processed=processed,
                            events_produced=self._produced,
                        )
                self.log.info(
                    "replay.pass.complete",
                    pass_no=pass_no,
                    events_produced=self._produced,
                )
                if not loop:
                    break
        except KeyboardInterrupt:
            self.log.warning("replay.interrupted", events_produced=self._produced)
        finally:
            self.flush_and_close()

    def _events_for_row(self, row: dict[str, Any]) -> Iterator[tuple[Any, str | None]]:
        """Yield ``(event, key)`` pairs for a single input row.

        The default implementation emits exactly one event built by
        :meth:`_build_event`. Producers that emit an event *sequence* per row
        (e.g. the order lifecycle) override this method.

        Args:
            row: A raw input record.

        Yields:
            Tuples of ``(event_dataclass, partition_key)``.
        """
        event = self._build_event(row)
        yield event, getattr(event, "order_id", None)

    # ----------------------------------------------------------------- teardown

    def flush_and_close(self) -> None:
        """Flush any buffered messages and log a shutdown summary."""
        remaining = self._producer.flush(timeout=30)
        self.log.info(
            "producer.closed",
            total_produced=self._produced,
            unflushed=remaining,
        )
