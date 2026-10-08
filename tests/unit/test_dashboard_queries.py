"""Unit tests for the dashboard's Delta queries (:mod:`dashboard.queries`)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
from pyspark.sql import SparkSession

import queries

_WINDOWS_SCHEMA = (
    "window_start timestamp, window_end timestamp, customer_state string, "
    "product_category string, total_orders long, total_revenue double, "
    "unique_customers long"
)
_RATE_SCHEMA = (
    "window_start timestamp, window_end timestamp, orders_per_minute double, "
    "revenue_rate double, top_state string, top_category string"
)


def _recent_minute() -> datetime:
    """Return a minute-aligned naive UTC timestamp a few minutes ago."""
    now = datetime.now(timezone.utc).replace(tzinfo=None, second=0, microsecond=0)
    return now - timedelta(minutes=5)


def _write_gold(spark: SparkSession, gold_path: str) -> None:
    """Write small Gold windows and rate tables at ``gold_path``."""
    start = _recent_minute()
    end = start + timedelta(minutes=1)
    spark.createDataFrame(
        [
            (start, end, "SP", "esporte", 3, 300.0, 3),
            (start, end, "RJ", "beleza", 1, 50.0, 1),
        ],
        _WINDOWS_SCHEMA,
    ).write.format("delta").save(gold_path)
    spark.createDataFrame(
        [(start, start + timedelta(minutes=5), 0.8, 70.0, "SP", "esporte")],
        _RATE_SCHEMA,
    ).write.format("delta").save(f"{gold_path}_rate")


def test_queries_return_empty_when_gold_is_missing(
    spark: SparkSession, tmp_delta_path: str
) -> None:
    """Without a Gold table every query degrades to an empty result."""
    assert queries.get_kpi_summary(spark, tmp_delta_path)["total_orders"] == 0
    assert queries.get_recent_windows(spark, tmp_delta_path).empty
    assert queries.get_orders_timeseries(spark, tmp_delta_path).empty


def test_timestamp_queries_convert_to_pandas(
    spark: SparkSession, tmp_delta_path: str
) -> None:
    """Queries returning timestamp columns convert to pandas datetimes.

    PySpark 3.4's non-Arrow toPandas fails on pandas 2 timestamps; this is
    the path behind the dashboard's time series and recent-windows table.
    """
    _write_gold(spark, tmp_delta_path)

    recent = queries.get_recent_windows(spark, tmp_delta_path)
    series = queries.get_orders_timeseries(spark, tmp_delta_path, minutes=30)

    assert len(recent) == 2
    assert pd.api.types.is_datetime64_any_dtype(recent["window_start"])
    assert len(series) == 1
    assert series["orders_per_minute"].iloc[0] == 0.8


def test_kpi_summary_and_rankings(spark: SparkSession, tmp_delta_path: str) -> None:
    """KPIs sum the tumbling windows; rankings order by the metric."""
    _write_gold(spark, tmp_delta_path)

    kpis = queries.get_kpi_summary(spark, tmp_delta_path)
    by_state = queries.get_revenue_by_state(spark, tmp_delta_path)

    assert kpis["total_orders"] == 4
    assert kpis["total_revenue"] == 350.0
    assert kpis["avg_ticket"] == 87.5
    assert list(by_state["customer_state"]) == ["SP", "RJ"]
