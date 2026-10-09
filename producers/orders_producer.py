"""Orders producer: replays the Olist orders dataset as a lifecycle event stream.

The producer joins three Olist CSVs in memory:

* ``olist_orders_dataset.csv``    — one row per order,
* ``olist_order_items_dataset.csv`` — line items (value + seller),
* ``olist_customers_dataset.csv``   — customer → state mapping,

and, when available, ``olist_products_dataset.csv`` to resolve the product
category. For every order it emits a realistic lifecycle:

``ORDER_CREATED → ORDER_APPROVED → ORDER_SHIPPED → ORDER_DELIVERED``

with a configurable probability (default 12%) of ``ORDER_CANCELED`` after
approval. Each event is timestamped at emission time so that the downstream
windowed aggregations always reflect *live* activity. The order's business
dates from the CSV (purchase, estimated delivery, delivery to the customer)
travel on every event as optional fields, for delivery-SLA analysis.
"""

from __future__ import annotations

import dataclasses
import os
import random
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from base_producer import BaseProducer, EventMetadata, configure_logging

# Olist orders never have a payment value below this when items are missing.
_DEFAULT_PAYMENT_VALUE = 0.0


@dataclasses.dataclass
class OrderEvent:
    """An order lifecycle event matching ``order_event.avsc``.

    Attributes:
        event_id: Globally unique event identifier (UUID4).
        event_type: One of the ``OrderEventType`` enum symbols.
        order_id: Olist order identifier.
        customer_id: Olist customer identifier.
        customer_state: Two-letter Brazilian state code.
        event_timestamp: Event time in epoch milliseconds.
        metadata: Producer envelope metadata.
        seller_id: First seller on the order, if known.
        payment_value: Total order value, if known.
        product_category: Resolved product category, if known.
        purchase_ts: CSV ``order_purchase_timestamp`` (epoch ms), if present.
        estimated_delivery_ts: CSV ``order_estimated_delivery_date`` (epoch
            ms), if present.
        delivered_customer_ts: CSV ``order_delivered_customer_date`` (epoch
            ms), if present.
    """

    event_id: str
    event_type: str
    order_id: str
    customer_id: str
    customer_state: str
    event_timestamp: int
    metadata: EventMetadata
    seller_id: str | None = None
    payment_value: float | None = None
    product_category: str | None = None
    purchase_ts: int | None = None
    estimated_delivery_ts: int | None = None
    delivered_customer_ts: int | None = None


def _csv_ts_ms(value: str | None) -> int | None:
    """Parse an Olist CSV timestamp (``YYYY-MM-DD[ HH:MM:SS]``, UTC) to epoch ms.

    Args:
        value: The CSV cell; empty or missing for orders that never reached
            that milestone.

    Returns:
        Epoch milliseconds, or ``None`` when the cell is empty or malformed.
    """
    if not value or not value.strip():
        return None
    text = value.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(text, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        return int(parsed.timestamp() * 1000)
    return None


class OrdersProducer(BaseProducer):
    """Produces Olist order lifecycle events to the ``orders-raw`` topic."""

    def __init__(
        self,
        bootstrap_servers: str,
        schema_registry_url: str,
        topic: str,
        events_per_second: int = 10,
        *,
        data_dir: str | os.PathLike[str] = "data/raw",
        cancellation_rate: float = 0.12,
        **kwargs: Any,
    ) -> None:
        """Initialize the orders producer and load the join dimensions.

        Args:
            bootstrap_servers: Kafka bootstrap servers.
            schema_registry_url: Schema Registry URL.
            topic: Destination topic (``orders-raw``).
            events_per_second: Target publish rate.
            data_dir: Directory containing the Olist CSVs.
            cancellation_rate: Probability of cancellation after approval.
            **kwargs: Forwarded to :class:`BaseProducer`.
        """
        super().__init__(
            bootstrap_servers,
            schema_registry_url,
            topic,
            events_per_second,
            source="orders-producer",
            **kwargs,
        )
        self.data_dir = Path(data_dir)
        self.cancellation_rate = cancellation_rate
        self._customers: dict[str, str] = {}
        self._items: dict[str, tuple[float, str | None]] = {}
        self._products: dict[str, str] = {}
        self._order_categories: dict[str, str | None] = {}
        self._load_dimensions()

    def _default_schema_path(self) -> Path:
        """Return the path to ``order_event.avsc``."""
        return Path(__file__).parent / "schemas" / "order_event.avsc"

    # ----------------------------------------------------------- dimension load

    def _load_dimensions(self) -> None:
        """Load customers, order items and (optionally) products into memory."""
        import csv

        customers_path = self.data_dir / "olist_customers_dataset.csv"
        with open(customers_path, newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                self._customers[row["customer_id"]] = row.get("customer_state", "NA")

        items_path = self.data_dir / "olist_order_items_dataset.csv"
        totals: dict[str, float] = defaultdict(float)
        sellers: dict[str, str | None] = {}
        first_product: dict[str, str] = {}
        with open(items_path, newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                oid = row["order_id"]
                totals[oid] += float(row.get("price") or 0.0) + float(
                    row.get("freight_value") or 0.0
                )
                sellers.setdefault(oid, row.get("seller_id") or None)
                first_product.setdefault(oid, row.get("product_id") or "")
        self._items = {oid: (totals[oid], sellers.get(oid)) for oid in totals}

        # Optional products dimension for category enrichment.
        products_path = self.data_dir / "olist_products_dataset.csv"
        if products_path.exists():
            with open(products_path, newline="", encoding="utf-8") as handle:
                for row in csv.DictReader(handle):
                    cat = row.get("product_category_name") or None
                    if cat:
                        self._products[row["product_id"]] = cat
            for oid, pid in first_product.items():
                self._order_categories[oid] = self._products.get(pid)

        self.log.info(
            "dimensions.loaded",
            customers=len(self._customers),
            orders_with_items=len(self._items),
            products=len(self._products),
        )

    # -------------------------------------------------------------- event build

    def _iter_rows(self, csv_path: str | os.PathLike[str]) -> Iterator[dict[str, Any]]:
        """Stream the orders CSV, enriched with the in-memory dimensions.

        Args:
            csv_path: Path to ``olist_orders_dataset.csv``.

        Yields:
            One enriched order dict per order.
        """
        import csv

        with open(csv_path, newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                oid = row["order_id"]
                value, seller_id = self._items.get(oid, (_DEFAULT_PAYMENT_VALUE, None))
                yield {
                    "order_id": oid,
                    "customer_id": row["customer_id"],
                    "customer_state": self._customers.get(row["customer_id"], "NA"),
                    "seller_id": seller_id,
                    "payment_value": round(value, 2),
                    "product_category": self._order_categories.get(oid),
                    # Business dates from the CSV, carried on every event of
                    # the order. Older sample CSVs lack them: None.
                    "purchase_ts": _csv_ts_ms(row.get("order_purchase_timestamp")),
                    "estimated_delivery_ts": _csv_ts_ms(
                        row.get("order_estimated_delivery_date")
                    ),
                    "delivered_customer_ts": _csv_ts_ms(
                        row.get("order_delivered_customer_date")
                    ),
                }

    def _new_event(self, order: dict[str, Any], event_type: str) -> OrderEvent:
        """Construct a single :class:`OrderEvent` for the given lifecycle stage.

        Args:
            order: The enriched order record.
            event_type: The ``OrderEventType`` symbol to emit.

        Returns:
            A fully populated :class:`OrderEvent`.
        """
        return OrderEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            order_id=order["order_id"],
            customer_id=order["customer_id"],
            customer_state=order["customer_state"],
            event_timestamp=int(time.time() * 1000),
            metadata=EventMetadata(source=self.source, producer_id=self.producer_id),
            seller_id=order["seller_id"],
            payment_value=order["payment_value"],
            product_category=order["product_category"],
            purchase_ts=order.get("purchase_ts"),
            estimated_delivery_ts=order.get("estimated_delivery_ts"),
            delivered_customer_ts=order.get("delivered_customer_ts"),
        )

    def _build_event(self, row: dict[str, Any]) -> OrderEvent:
        """Build a single ``ORDER_CREATED`` event (used by the base contract).

        Args:
            row: An enriched order record.

        Returns:
            An :class:`OrderEvent` of type ``ORDER_CREATED``.
        """
        return self._new_event(row, "ORDER_CREATED")

    def _events_for_row(
        self, row: dict[str, Any]
    ) -> Iterator[tuple[OrderEvent, str | None]]:
        """Emit the full lifecycle sequence for a single order.

        The sequence is ``CREATED → APPROVED`` followed by either the happy path
        (``SHIPPED → DELIVERED``) or, with probability ``cancellation_rate``,
        ``CANCELED``.

        Args:
            row: An enriched order record.

        Yields:
            ``(OrderEvent, order_id)`` tuples in lifecycle order.
        """
        key = row["order_id"]
        yield self._new_event(row, "ORDER_CREATED"), key
        yield self._new_event(row, "ORDER_APPROVED"), key

        if random.random() < self.cancellation_rate:
            yield self._new_event(row, "ORDER_CANCELED"), key
            return

        yield self._new_event(row, "ORDER_SHIPPED"), key
        yield self._new_event(row, "ORDER_DELIVERED"), key


def main() -> None:
    """Entry point: build an :class:`OrdersProducer` from env vars and run it."""
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    data_dir = os.environ.get("DATA_DIR", "data/raw")
    producer = OrdersProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        schema_registry_url=os.environ.get(
            "SCHEMA_REGISTRY_URL", "http://localhost:8081"
        ),
        topic=os.environ.get("TOPIC_ORDERS_RAW", "orders-raw"),
        events_per_second=int(os.environ.get("EVENTS_PER_SECOND", "10")),
        data_dir=data_dir,
        cancellation_rate=float(os.environ.get("CANCELLATION_RATE", "0.12")),
    )
    loop = os.environ.get("PRODUCER_LOOP", "true").lower() in {"1", "true", "yes"}
    producer.produce_from_csv(Path(data_dir) / "olist_orders_dataset.csv", loop=loop)


if __name__ == "__main__":
    main()
