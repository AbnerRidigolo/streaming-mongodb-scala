"""Unit tests for the MongoDB order lookup (:mod:`dashboard.order_lookup`).

A small in-memory fake stands in for pymongo's Collection, so no MongoDB runs.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

import order_lookup

_T0 = datetime(2026, 10, 8, 17, 58, tzinfo=timezone.utc)


class FakeCollection:
    """Equality filters, ``{"_id": 0}`` projection, sort and limit."""

    def __init__(self, docs: list[dict[str, Any]]) -> None:
        self.docs = docs
        self.queries: list[Mapping[str, Any]] = []

    def _match(self, filter: Mapping[str, Any]) -> list[dict[str, Any]]:
        self.queries.append(dict(filter))
        return [
            {k: v for k, v in d.items() if k != "_id"}
            for d in self.docs
            if all(d.get(k) == v for k, v in filter.items())
        ]

    def find_one(
        self, filter: Mapping[str, Any], projection: Mapping[str, Any]
    ) -> dict[str, Any] | None:
        assert projection == {"_id": 0}
        found = self._match(filter)
        return found[0] if found else None

    def find(
        self,
        filter: Mapping[str, Any],
        projection: Mapping[str, Any],
        sort: list[tuple[str, int]] | None = None,
        limit: int = 0,
    ) -> Iterable[dict[str, Any]]:
        assert projection == {"_id": 0}
        found = self._match(filter)
        for key, direction in reversed(sort or []):
            found.sort(key=lambda d: d[key], reverse=direction < 0)
        return found[:limit] if limit else found


def _order(**overrides: Any) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "_id": "65f0c0ffee",
        "order_id": "order_000000",
        "status": "DELIVERED",
        "customer_state": "DF",
        "order_value": 224.81,
        "paid_amount": 223.09,
        "payment_count": 1,
        "payment_types": ["BOLETO"],
        "created_at": _T0,
        "approved_at": _T0 + timedelta(seconds=1),
        "paid_at": _T0 + timedelta(minutes=1),
        # Delivery producer ahead of the orders producer: shipped "before" created.
        "shipped_at": _T0 - timedelta(seconds=2),
        "delivered_at": _T0 + timedelta(seconds=30),
        "canceled_at": None,
        "updated_at": _T0 + timedelta(minutes=1),
    }
    doc.update(overrides)
    return doc


def test_normalize_order_id() -> None:
    assert order_lookup.normalize_order_id("  order_000001 ") == "order_000001"
    assert order_lookup.normalize_order_id("   ") is None
    assert order_lookup.normalize_order_id(None) is None


def test_fetch_order_by_id_hides_object_id() -> None:
    orders = FakeCollection([_order(), _order(order_id="order_000001")])
    found = order_lookup.fetch_order(orders, "order_000001")
    assert found is not None
    assert found["order_id"] == "order_000001"
    assert "_id" not in found
    assert orders.queries == [{"order_id": "order_000001"}]


def test_fetch_order_missing_returns_none() -> None:
    assert order_lookup.fetch_order(FakeCollection([_order()]), "nope") is None


def test_timeline_is_chronological_and_skips_missing_milestones() -> None:
    timeline = order_lookup.build_timeline(_order())
    assert list(timeline.columns) == ["marco", "quando"]
    assert list(timeline["marco"]) == [
        "Enviado",
        "Pedido criado",
        "Pedido aprovado",
        "Entregue",
        "Pagamento recebido",
    ]
    assert "Cancelado" not in set(timeline["marco"])


def test_timeline_of_order_known_only_from_payments() -> None:
    # order-status can hold a state built only from a payment event.
    doc = {"order_id": "order_000001", "status": "PAID", "paid_at": _T0}
    timeline = order_lookup.build_timeline(doc)
    assert list(timeline["marco"]) == ["Pagamento recebido"]
    assert order_lookup.build_timeline({"order_id": "x"}).empty


def test_payment_summary() -> None:
    summary = order_lookup.payment_summary(_order())
    assert summary == {
        "order_value": 224.81,
        "paid_amount": 223.09,
        "payment_count": 1,
        "payment_types": ["BOLETO"],
        "difference": -1.72,
    }


def test_payment_summary_without_order_value() -> None:
    summary = order_lookup.payment_summary(
        {"order_id": "x", "order_value": None, "paid_amount": 10.0}
    )
    assert summary["difference"] is None
    assert summary["payment_count"] == 0
    assert summary["payment_types"] == []


def test_fetch_alerts_oldest_first() -> None:
    alerts = FakeCollection(
        [
            {
                "alert_id": "DELIVERED_AFTER_CANCELLATION:order_000000",
                "order_id": "order_000000",
                "alert_type": "DELIVERED_AFTER_CANCELLATION",
                "detected_at": _T0 + timedelta(minutes=5),
                "message": "late",
            },
            {
                "alert_id": "PAYMENT_TIMEOUT:order_000000",
                "order_id": "order_000000",
                "alert_type": "PAYMENT_TIMEOUT",
                "detected_at": _T0,
                "message": "early",
            },
            {
                "alert_id": "PAYMENT_TIMEOUT:order_000009",
                "order_id": "order_000009",
                "alert_type": "PAYMENT_TIMEOUT",
                "detected_at": _T0,
                "message": "other order",
            },
        ]
    )
    found = order_lookup.fetch_alerts(alerts, "order_000000")
    assert [a["message"] for a in found] == ["early", "late"]

    frame = order_lookup.alerts_frame(found)
    assert list(frame.columns) == ["tipo", "detectado_em", "mensagem"]
    assert list(frame["tipo"]) == ["PAYMENT_TIMEOUT", "DELIVERED_AFTER_CANCELLATION"]
    assert order_lookup.alerts_frame([]).empty


def test_recent_orders_filter_sort_and_limit() -> None:
    orders = FakeCollection(
        [
            _order(order_id=f"order_{i:06d}", updated_at=_T0 + timedelta(seconds=i))
            for i in range(5)
        ]
        + [_order(order_id="order_canceled", status="CANCELED")]
    )
    recent = order_lookup.fetch_recent_orders(orders, "DELIVERED", limit=3)
    assert [d["order_id"] for d in recent] == [
        "order_000004",
        "order_000003",
        "order_000002",
    ]
    assert orders.queries[-1] == {"status": "DELIVERED"}

    everything = order_lookup.fetch_recent_orders(orders, None, limit=0)
    assert len(everything) == 6
    assert orders.queries[-1] == {}
