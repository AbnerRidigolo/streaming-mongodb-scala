"""Unit tests for the orders producer lifecycle logic (no Kafka required)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from orders_producer import OrderEvent, OrdersProducer


@pytest.fixture
def producer() -> OrdersProducer:
    """Build an OrdersProducer without running the network-bound __init__.

    Returns:
        A partially-initialized producer with just the attributes the lifecycle
        methods rely on.
    """
    inst = OrdersProducer.__new__(OrdersProducer)
    inst.source = "orders-producer"
    inst.producer_id = "test-001"
    inst.cancellation_rate = 0.12
    inst._customers = {"cust_1": "SP", "cust_2": "RJ"}
    inst._items = {"order_1": (150.0, "seller_9"), "order_2": (90.0, "seller_3")}
    inst._products = {}
    inst._order_categories = {"order_1": "beleza_saude", "order_2": None}
    return inst


def test_new_event_has_expected_fields(producer: OrdersProducer) -> None:
    """A built event carries the right ids, type and an int timestamp."""
    order = {
        "order_id": "order_1",
        "customer_id": "cust_1",
        "customer_state": "SP",
        "seller_id": "seller_9",
        "payment_value": 150.0,
        "product_category": "beleza_saude",
    }
    event = producer._new_event(order, "ORDER_CREATED")

    assert isinstance(event, OrderEvent)
    assert event.event_type == "ORDER_CREATED"
    assert event.order_id == "order_1"
    assert event.customer_state == "SP"
    assert isinstance(event.event_timestamp, int)
    assert event.metadata.producer_id == "test-001"


def test_lifecycle_happy_path_sequence(producer: OrdersProducer) -> None:
    """With no cancellation the sequence is CREATED→APPROVED→SHIPPED→DELIVERED."""
    order = {
        "order_id": "order_1",
        "customer_id": "cust_1",
        "customer_state": "SP",
        "seller_id": "seller_9",
        "payment_value": 150.0,
        "product_category": "beleza_saude",
    }
    with patch("orders_producer.random.random", return_value=0.99):
        events = [e.event_type for e, _ in producer._events_for_row(order)]

    assert events == [
        "ORDER_CREATED",
        "ORDER_APPROVED",
        "ORDER_SHIPPED",
        "ORDER_DELIVERED",
    ]


def test_lifecycle_cancellation_path(producer: OrdersProducer) -> None:
    """When the dice roll is below the rate the order is canceled after approval."""
    order = {
        "order_id": "order_2",
        "customer_id": "cust_2",
        "customer_state": "RJ",
        "seller_id": "seller_3",
        "payment_value": 90.0,
        "product_category": None,
    }
    with patch("orders_producer.random.random", return_value=0.01):
        events = [e.event_type for e, _ in producer._events_for_row(order)]

    assert events == ["ORDER_CREATED", "ORDER_APPROVED", "ORDER_CANCELED"]


def test_iter_rows_enriches_from_dimensions(
    producer: OrdersProducer, tmp_path: Path
) -> None:
    """_iter_rows joins the in-memory dimensions onto each order row."""
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,customer_id\norder_1,cust_1\norder_2,cust_2\n",
        encoding="utf-8",
    )

    rows = list(producer._iter_rows(csv_file))

    assert rows[0]["customer_state"] == "SP"
    assert rows[0]["payment_value"] == 150.0
    assert rows[0]["seller_id"] == "seller_9"
    assert rows[0]["product_category"] == "beleza_saude"
    assert rows[1]["customer_state"] == "RJ"
