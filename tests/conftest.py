"""Shared pytest fixtures for the Olist streaming pipeline test suite.

Provides a session-scoped local Delta-enabled :class:`SparkSession`, temporary
Delta paths, synthetic order events and a static customers DataFrame. The
``sys.path`` manipulation lets the test modules import the producer and
spark-job modules with their script-style (bare) imports.
"""

from __future__ import annotations

import sys
import uuid
from pathlib import Path
from typing import Any, Callable

import pytest

# --- Make project packages + script-style modules importable -----------------
_ROOT = Path(__file__).resolve().parent.parent
for _p in (_ROOT, _ROOT / "spark_jobs", _ROOT / "producers", _ROOT / "dashboard"):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from pyspark.sql import DataFrame, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

_STATES = ["SP", "RJ", "MG", "ES", "RS", "PR", "SC", "BA", "PE", "CE"]
_EVENT_TYPES = [
    "ORDER_CREATED",
    "ORDER_APPROVED",
    "ORDER_SHIPPED",
    "ORDER_DELIVERED",
    "ORDER_CANCELED",
]
_CATEGORIES = ["beleza_saude", "informatica", "cama_mesa_banho", "esporte"]
# Fixed base event time (ms) so window tests are deterministic.
_BASE_TS_MS = 1_700_000_000_000


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a session-scoped local Delta SparkSession (no Kafka, 2 threads).

    Yields:
        A configured :class:`SparkSession`.
    """
    from spark_session import get_test_session

    session = get_test_session("olist-tests")
    yield session
    session.stop()


@pytest.fixture
def tmp_delta_path(tmp_path: Path) -> str:
    """Return a fresh temporary path for a Delta table.

    Args:
        tmp_path: pytest's per-test temporary directory.

    Returns:
        A string path inside ``tmp_path`` for use as a Delta table location.
    """
    return str(tmp_path / "delta_table")


@pytest.fixture
def sample_order_events() -> list[dict[str, Any]]:
    """Return 50 synthetic order events covering all types and many states.

    Returns:
        A list of 50 event dicts shaped like the OrderEvent schema, with an
        added ``event_timestamp`` (ms).
    """
    events: list[dict[str, Any]] = []
    for i in range(50):
        events.append(
            {
                "event_id": str(uuid.uuid4()),
                "event_type": _EVENT_TYPES[i % len(_EVENT_TYPES)],
                "order_id": f"order_{i % 20:04d}",
                "customer_id": f"cust_{i % 10:04d}",
                "seller_id": f"seller_{i % 7:03d}",
                "payment_value": float(20 + (i * 13) % 600),
                "product_category": _CATEGORIES[i % len(_CATEGORIES)],
                "customer_state": _STATES[i % len(_STATES)],
                # Spread events across ~3 minutes for window tests.
                "event_timestamp": _BASE_TS_MS + (i * 4000),
                "metadata": {
                    "source": "test",
                    "version": "1.0",
                    "producer_id": "pytest",
                },
            }
        )
    return events


@pytest.fixture
def sample_customers_df(spark: SparkSession) -> DataFrame:
    """Return a static DataFrame of 10 customers from different states.

    Args:
        spark: The session SparkSession.

    Returns:
        A DataFrame with ``customer_id``, ``customer_unique_id``,
        ``customer_zip_code_prefix``, ``customer_city`` and ``customer_state``.
    """
    rows = [
        (
            f"cust_{i:04d}",
            str(uuid.uuid4()),
            10000 + i,
            f"city_{i}",
            _STATES[i],
        )
        for i in range(10)
    ]
    return spark.createDataFrame(
        rows,
        schema=[
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ],
    )


@pytest.fixture
def make_events_df(
    spark: SparkSession,
) -> Callable[[list[dict[str, Any]]], DataFrame]:
    """Return a factory that builds a Silver-shaped events DataFrame.

    The returned DataFrame includes the derived ``event_ts`` (timestamp) and
    ``is_high_value`` columns used by the enrichment/aggregation tests.

    Args:
        spark: The session SparkSession.

    Returns:
        A callable ``make_events_df(events) -> DataFrame``.
    """

    def _make(events: list[dict[str, Any]]) -> DataFrame:
        flat = [{k: v for k, v in e.items() if k != "metadata"} for e in events]
        df = spark.createDataFrame(flat)
        return (
            df.withColumn(
                "event_ts", (F.col("event_timestamp") / 1000).cast("timestamp")
            )
            .withColumn("is_high_value", F.col("payment_value") > 500)
            .withColumn("_date", F.to_date("event_ts"))
        )

    return _make
