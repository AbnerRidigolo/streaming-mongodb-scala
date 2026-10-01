"""Delivery producer: simulates parcel tracking events to ``delivery-events``.

For every order it emits a delivery progression:

``PICKED_UP → IN_TRANSIT → OUT_FOR_DELIVERY → DELIVERED``

Each event carries randomized GPS coordinates jittered around the centroid of
the customer's Brazilian state, so a map visualization shows realistic movement.
This producer has no dedicated ``.avsc`` file on disk — its Avro schema is
defined inline and registered automatically with Schema Registry.
"""

from __future__ import annotations

import dataclasses
import json
import os
import random
import time
import uuid
from pathlib import Path
from typing import Any, Iterator

from base_producer import BaseProducer, EventMetadata, configure_logging

# Approximate (latitude, longitude) centroid per Brazilian state.
_STATE_CENTROIDS: dict[str, tuple[float, float]] = {
    "AC": (-9.02, -70.81),
    "AL": (-9.57, -36.78),
    "AP": (1.41, -51.77),
    "AM": (-3.42, -65.86),
    "BA": (-12.96, -41.71),
    "CE": (-5.20, -39.53),
    "DF": (-15.78, -47.93),
    "ES": (-19.18, -40.31),
    "GO": (-15.83, -49.84),
    "MA": (-5.42, -45.44),
    "MT": (-12.64, -55.42),
    "MS": (-20.51, -54.54),
    "MG": (-18.10, -44.38),
    "PA": (-3.79, -52.48),
    "PB": (-7.28, -36.72),
    "PR": (-24.89, -51.55),
    "PE": (-8.38, -37.86),
    "PI": (-7.72, -42.73),
    "RJ": (-22.91, -43.21),
    "RN": (-5.81, -36.59),
    "RS": (-30.17, -53.50),
    "RO": (-10.83, -63.34),
    "RR": (1.99, -61.33),
    "SC": (-27.45, -50.95),
    "SP": (-22.19, -48.79),
    "SE": (-10.57, -37.45),
    "TO": (-9.46, -48.26),
    "NA": (-14.24, -51.93),  # Brazil centroid fallback
}

_DELIVERY_SCHEMA = json.dumps(
    {
        "type": "record",
        "name": "DeliveryEvent",
        "namespace": "br.com.olist.events",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "order_id", "type": "string"},
            {
                "name": "delivery_status",
                "type": {
                    "type": "enum",
                    "name": "DeliveryStatus",
                    "symbols": [
                        "PICKED_UP",
                        "IN_TRANSIT",
                        "OUT_FOR_DELIVERY",
                        "DELIVERED",
                    ],
                },
            },
            {"name": "customer_state", "type": "string"},
            {"name": "latitude", "type": "double"},
            {"name": "longitude", "type": "double"},
            {
                "name": "event_timestamp",
                "type": {"type": "long", "logicalType": "timestamp-millis"},
            },
            {
                "name": "metadata",
                "type": {
                    "type": "record",
                    "name": "DeliveryMetadata",
                    "fields": [
                        {"name": "source", "type": "string"},
                        {"name": "version", "type": "string", "default": "1.0"},
                        {"name": "producer_id", "type": "string"},
                    ],
                },
            },
        ],
    }
)

_STATUS_SEQUENCE = ("PICKED_UP", "IN_TRANSIT", "OUT_FOR_DELIVERY", "DELIVERED")


@dataclasses.dataclass
class DeliveryEvent:
    """A delivery tracking event (inline Avro schema).

    Attributes:
        event_id: Globally unique event identifier (UUID4).
        order_id: Olist order identifier being delivered.
        delivery_status: One of the ``DeliveryStatus`` enum symbols.
        customer_state: Destination Brazilian state.
        latitude: Current latitude (jittered around the state centroid).
        longitude: Current longitude (jittered around the state centroid).
        event_timestamp: Event time in epoch milliseconds.
        metadata: Producer envelope metadata.
    """

    event_id: str
    order_id: str
    delivery_status: str
    customer_state: str
    latitude: float
    longitude: float
    event_timestamp: int
    metadata: EventMetadata


class DeliveryProducer(BaseProducer):
    """Produces simulated delivery tracking events to ``delivery-events``."""

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        events_per_second: int = 10,
        *,
        data_dir: str | os.PathLike[str] = "data/raw",
        **kwargs: Any,
    ) -> None:
        """Initialize the delivery producer with the inline Avro schema.

        Args:
            bootstrap_servers: Kafka bootstrap servers.
            schema_registry_url: Schema Registry URL.
            topic: Destination topic (``delivery-events``).
            events_per_second: Target publish rate.
            data_dir: Directory containing the Olist CSVs.
            **kwargs: Forwarded to :class:`BaseProducer`.
        """
        super().__init__(
            bootstrap_servers,
            schema_registry_url,
            topic,
            events_per_second,
            schema_str=_DELIVERY_SCHEMA,
            source="delivery-producer",
            **kwargs,
        )
        self.data_dir = Path(data_dir)
        self._customers: dict[str, str] = {}
        self._load_customers()

    def _default_schema_path(self) -> Path:
        """Not used — the delivery schema is provided inline."""
        raise NotImplementedError("DeliveryProducer uses an inline schema")

    def _load_customers(self) -> None:
        """Load the customer → state mapping used to place GPS coordinates."""
        import csv

        path = self.data_dir / "olist_customers_dataset.csv"
        with open(path, newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                self._customers[row["customer_id"]] = row.get("customer_state", "NA")
        self.log.info("customers.loaded", count=len(self._customers))

    def _random_coords(self, state: str) -> tuple[float, float]:
        """Return jittered GPS coordinates near a state's centroid.

        Args:
            state: Two-letter Brazilian state code.

        Returns:
            A ``(latitude, longitude)`` tuple.
        """
        lat, lon = _STATE_CENTROIDS.get(state, _STATE_CENTROIDS["NA"])
        return (
            round(lat + random.uniform(-1.5, 1.5), 6),
            round(lon + random.uniform(-1.5, 1.5), 6),
        )

    def _new_event(self, order_id: str, state: str, status: str) -> DeliveryEvent:
        """Construct a single :class:`DeliveryEvent`.

        Args:
            order_id: The order being delivered.
            state: Destination state code.
            status: The ``DeliveryStatus`` symbol.

        Returns:
            A fully populated :class:`DeliveryEvent`.
        """
        lat, lon = self._random_coords(state)
        return DeliveryEvent(
            event_id=str(uuid.uuid4()),
            order_id=order_id,
            delivery_status=status,
            customer_state=state,
            latitude=lat,
            longitude=lon,
            event_timestamp=int(time.time() * 1000),
            metadata=EventMetadata(source=self.source, producer_id=self.producer_id),
        )

    def _build_event(self, row: dict[str, Any]) -> DeliveryEvent:
        """Build a single ``PICKED_UP`` event (base-contract entry point).

        Args:
            row: A row of ``olist_orders_dataset.csv``.

        Returns:
            A :class:`DeliveryEvent` of status ``PICKED_UP``.
        """
        state = self._customers.get(row["customer_id"], "NA")
        return self._new_event(row["order_id"], state, "PICKED_UP")

    def _events_for_row(
        self, row: dict[str, Any]
    ) -> Iterator[tuple[DeliveryEvent, str | None]]:
        """Emit the full delivery progression for a single order.

        Args:
            row: An orders CSV row.

        Yields:
            ``(DeliveryEvent, order_id)`` tuples in delivery order.
        """
        order_id = row["order_id"]
        state = self._customers.get(row["customer_id"], "NA")
        for status in _STATUS_SEQUENCE:
            yield self._new_event(order_id, state, status), order_id


def main() -> None:
    """Entry point: build a :class:`DeliveryProducer` from env vars and run it."""
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    data_dir = os.environ.get("DATA_DIR", "data/raw")
    producer = DeliveryProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        schema_registry_url=os.environ.get(
            "SCHEMA_REGISTRY_URL", "http://localhost:8081"
        ),
        topic=os.environ.get("TOPIC_DELIVERY", "delivery-events"),
        events_per_second=int(os.environ.get("EVENTS_PER_SECOND", "10")),
        data_dir=data_dir,
    )
    loop = os.environ.get("PRODUCER_LOOP", "true").lower() in {"1", "true", "yes"}
    producer.produce_from_csv(Path(data_dir) / "olist_orders_dataset.csv", loop=loop)


if __name__ == "__main__":
    main()
