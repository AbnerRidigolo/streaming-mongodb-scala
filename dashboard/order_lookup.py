"""Order lookup against the MongoDB serving layer (database ``olist_serving``).

The Kafka Connect MongoDB sink keeps ``order_status`` (one document per
``order_id``, the latest state from order-status-service) and ``order_alerts``
(one document per ``alert_id``). These functions only need ``find_one``/``find``
with pymongo's signature, so tests pass a fake collection instead of MongoDB.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping, Protocol

import pandas as pd

# Lifecycle milestones in the order the business expects them. Display order
# follows the timestamps instead, because the three producers run at
# independent paces and the times across sources are not coherent.
MILESTONES: list[tuple[str, str]] = [
    ("created_at", "Pedido criado"),
    ("approved_at", "Pedido aprovado"),
    ("paid_at", "Pagamento recebido"),
    ("shipped_at", "Enviado"),
    ("delivered_at", "Entregue"),
    ("canceled_at", "Cancelado"),
]

STATUSES = ["CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELED"]

# Never return MongoDB's internal ObjectId to the page.
_PROJECTION = {"_id": 0}


class Collection(Protocol):
    """The subset of :class:`pymongo.collection.Collection` used here."""

    def find_one(
        self, filter: Mapping[str, Any], projection: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        ...

    def find(
        self,
        filter: Mapping[str, Any],
        projection: Mapping[str, Any],
        sort: list[tuple[str, int]] | None = None,
        limit: int = 0,
    ) -> Iterable[dict[str, Any]]:
        ...


def normalize_order_id(raw: str | None) -> str | None:
    """Return the trimmed order id, or ``None`` when nothing was typed.

    Args:
        raw: Text typed by the user.

    Returns:
        The order id to look up, or ``None``.
    """
    order_id = (raw or "").strip()
    return order_id or None


def fetch_order(orders: Collection, order_id: str) -> dict[str, Any] | None:
    """Fetch the current state of one order.

    Args:
        orders: The ``order_status`` collection.
        order_id: Order to look up (unique index on ``order_id``).

    Returns:
        The order document without ``_id``, or ``None`` if it is not there.
    """
    return orders.find_one({"order_id": order_id}, _PROJECTION)


def fetch_alerts(alerts: Collection, order_id: str) -> list[dict[str, Any]]:
    """Fetch the alerts raised for one order, oldest first.

    Args:
        alerts: The ``order_alerts`` collection.
        order_id: Order whose alerts to fetch (index on ``order_id``).

    Returns:
        Alert documents without ``_id``.
    """
    return list(
        alerts.find({"order_id": order_id}, _PROJECTION, sort=[("detected_at", 1)])
    )


def fetch_recent_orders(
    orders: Collection, status: str | None = None, limit: int = 20
) -> list[dict[str, Any]]:
    """Fetch the most recently updated orders, optionally of one status.

    With a status this is served by the ``{status: 1, updated_at: -1}`` index.

    Args:
        orders: The ``order_status`` collection.
        status: Lifecycle status to filter on, or ``None`` for all.
        limit: Maximum number of orders.

    Returns:
        Order documents without ``_id``, newest ``updated_at`` first.
    """
    query: dict[str, Any] = {"status": status} if status else {}
    return list(orders.find(query, _PROJECTION, sort=[("updated_at", -1)], limit=limit))


def build_timeline(order: Mapping[str, Any]) -> pd.DataFrame:
    """Turn an order's milestone timestamps into a chronological timeline.

    Args:
        order: An ``order_status`` document.

    Returns:
        A DataFrame with columns ``marco`` and ``quando``, sorted by time;
        milestones the order has not reached are left out.
    """
    rows = [
        {"marco": label, "quando": order[field]}
        for field, label in MILESTONES
        if isinstance(order.get(field), datetime)
    ]
    if not rows:
        return pd.DataFrame(columns=["marco", "quando"])
    return (
        pd.DataFrame(rows).sort_values("quando", kind="stable").reset_index(drop=True)
    )


def payment_summary(order: Mapping[str, Any]) -> dict[str, Any]:
    """Summarize what was paid against the order value.

    Args:
        order: An ``order_status`` document.

    Returns:
        ``order_value`` (``None`` until an order event arrives), ``paid_amount``,
        ``payment_count``, ``payment_types`` and ``difference``
        (paid minus order value, ``None`` without an order value).
    """
    order_value = order.get("order_value")
    paid_amount = float(order.get("paid_amount") or 0.0)
    return {
        "order_value": order_value,
        "paid_amount": paid_amount,
        "payment_count": int(order.get("payment_count") or 0),
        "payment_types": list(order.get("payment_types") or []),
        "difference": (
            round(paid_amount - order_value, 2) if order_value is not None else None
        ),
    }


def alerts_frame(alerts: list[dict[str, Any]]) -> pd.DataFrame:
    """Table of alerts for display.

    Args:
        alerts: Documents from :func:`fetch_alerts`.

    Returns:
        A DataFrame with ``tipo``, ``detectado_em`` and ``mensagem``.
    """
    columns = {
        "alert_type": "tipo",
        "detected_at": "detectado_em",
        "message": "mensagem",
    }
    if not alerts:
        return pd.DataFrame(columns=list(columns.values()))
    return pd.DataFrame(alerts)[list(columns)].rename(columns=columns)
