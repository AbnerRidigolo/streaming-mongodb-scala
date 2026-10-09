"""Unit tests for the orders producer lifecycle logic (no Kafka required)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from orders_producer import OrderEvent, OrdersProducer, _csv_ts_ms


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


def test_csv_ts_ms_parses_olist_dates() -> None:
    """CSV timestamps and plain dates are read as UTC; blanks become None."""
    assert _csv_ts_ms("2017-10-02 10:56:33") == 1506941793000
    assert _csv_ts_ms("2017-10-18") == 1508284800000
    assert _csv_ts_ms("2017-10-18 00:00:00") == 1508284800000
    assert _csv_ts_ms("") is None
    assert _csv_ts_ms("   ") is None
    assert _csv_ts_ms(None) is None
    assert _csv_ts_ms("not a date") is None


def test_business_dates_travel_on_every_event(
    producer: OrdersProducer, tmp_path: Path
) -> None:
    """Purchase, estimated and delivered dates go on all lifecycle events."""
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text(
        "order_id,customer_id,order_purchase_timestamp,"
        "order_delivered_customer_date,order_estimated_delivery_date\n"
        "order_1,cust_1,2017-10-02 10:56:33,2017-10-10 21:25:13,"
        "2017-10-18 00:00:00\n"
        "order_2,cust_2,2017-10-03 08:00:00,,2017-10-20 00:00:00\n",
        encoding="utf-8",
    )
    first, undelivered = producer._iter_rows(csv_file)

    with patch("orders_producer.random.random", return_value=0.99):
        events = [e for e, _ in producer._events_for_row(first)]
    assert len(events) == 4
    for event in events:
        assert event.purchase_ts == 1506941793000
        assert event.delivered_customer_ts == 1507670713000
        assert event.estimated_delivery_ts == 1508284800000

    event = producer._new_event(undelivered, "ORDER_CREATED")
    assert event.delivered_customer_ts is None
    assert event.estimated_delivery_ts == 1508457600000


def test_csv_without_business_dates_gives_none(
    producer: OrdersProducer, tmp_path: Path
) -> None:
    """Sample CSVs generated before these columns existed still work."""
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text("order_id,customer_id\norder_1,cust_1\n", encoding="utf-8")
    (row,) = producer._iter_rows(csv_file)
    event = producer._new_event(row, "ORDER_CREATED")
    assert event.purchase_ts is None
    assert event.estimated_delivery_ts is None
    assert event.delivered_customer_ts is None
