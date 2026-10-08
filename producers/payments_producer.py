"""Payments producer: replays the Olist payments dataset to ``payments-raw``.

Reads ``olist_order_payments_dataset.csv`` and emits one payment event per row,
linked to the originating ``order_id``. A small random delay (0–5 minutes,
simulated) is applied between the (logical) ``ORDER_CREATED`` and the payment to
mimic real checkout latency.
"""

from __future__ import annotations

import dataclasses
import os
import random
import time
import uuid
from pathlib import Path
from typing import Any, Iterator

from base_producer import BaseProducer, EventMetadata, configure_logging

# Olist raw payment_type strings → Avro enum symbols.
_PAYMENT_TYPE_MAP: dict[str, str] = {
    "credit_card": "CREDIT_CARD",
    "boleto": "BOLETO",
    "voucher": "VOUCHER",
    "debit_card": "DEBIT_CARD",
}
_MAX_SIMULATED_DELAY_SECONDS = 5 * 60


@dataclasses.dataclass
class PaymentEvent:
    """A payment event matching ``payment_event.avsc``.

    Attributes:
        event_id: Globally unique event identifier (UUID4).
        order_id: Olist order identifier this payment settles.
        payment_type: One of the ``PaymentType`` enum symbols.
        payment_value: Settled amount.
        installments: Number of installments (>= 1).
        event_timestamp: Event time in epoch milliseconds.
        metadata: Producer envelope metadata.
    """

    event_id: str
    order_id: str
    payment_type: str
    payment_value: float
    installments: int
    event_timestamp: int
    metadata: EventMetadata


class PaymentsProducer(BaseProducer):
    """Produces Olist payment events to the ``payments-raw`` topic."""

    def _default_schema_path(self) -> Path:
        """Return the path to ``payment_event.avsc``."""
        return Path(__file__).parent / "schemas" / "payment_event.avsc"

    def _build_event(self, row: dict[str, Any]) -> PaymentEvent:
        """Build a :class:`PaymentEvent` from a raw payments CSV row.

        Args:
            row: A row of ``olist_order_payments_dataset.csv``.

        Returns:
            A fully populated :class:`PaymentEvent`.
        """
        raw_type = (row.get("payment_type") or "").strip().lower()
        payment_type = _PAYMENT_TYPE_MAP.get(raw_type, "CREDIT_CARD")
        installments = int(float(row.get("payment_installments") or 1)) or 1
        return PaymentEvent(
            event_id=str(uuid.uuid4()),
            order_id=row["order_id"],
            payment_type=payment_type,
            payment_value=round(float(row.get("payment_value") or 0.0), 2),
            installments=installments,
            event_timestamp=int(time.time() * 1000),
            metadata=EventMetadata(source=self.source, producer_id=self.producer_id),
        )

    def _events_for_row(
        self, row: dict[str, Any]
    ) -> Iterator[tuple[PaymentEvent, str | None]]:
        """Emit a single payment event, with a simulated checkout delay.

        Args:
            row: A payments CSV row.

        Yields:
            ``(PaymentEvent, order_id)``.
        """
        # Simulated 0–5 min latency, scaled down so demos stay responsive.
        jitter_ms = random.randint(0, _MAX_SIMULATED_DELAY_SECONDS) * 1000
        event = self._build_event(row)
        event.event_timestamp += jitter_ms
        yield event, event.order_id


def main() -> None:
    """Entry point: build a :class:`PaymentsProducer` from env vars and run it."""
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    data_dir = os.environ.get("DATA_DIR", "data/raw")
    producer = PaymentsProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        schema_registry_url=os.environ.get(
            "SCHEMA_REGISTRY_URL", "http://localhost:8081"
        ),
        topic=os.environ.get("TOPIC_PAYMENTS_RAW", "payments-raw"),
        events_per_second=int(os.environ.get("EVENTS_PER_SECOND", "10")),
        source="payments-producer",
    )
    loop = os.environ.get("PRODUCER_LOOP", "true").lower() in {"1", "true", "yes"}
    producer.produce_from_csv(
        Path(data_dir) / "olist_order_payments_dataset.csv", loop=loop
    )


if __name__ == "__main__":
    main()
