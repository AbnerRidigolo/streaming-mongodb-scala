"""Delta Lake queries powering the Streamlit dashboard.

Every function is defensive: if the Gold table does not exist yet (the pipeline
has not produced output), it returns an empty result instead of raising, so the
dashboard can show a friendly "waiting for pipeline" state.

The Gold *windows* table is produced with a 1-minute window sliding every 30s,
so consecutive windows overlap. To avoid double counting in the aggregate KPIs
we keep only the minute-aligned (tumbling) windows via :func:`_tumbling`.
"""

from __future__ import annotations

import pandas as pd
import structlog
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = structlog.get_logger("dashboard.queries")

_EMPTY = pd.DataFrame()


def _rate_path(gold_path: str) -> str:
    """Return the Gold *rate* table path derived from the windows path.

    Args:
        gold_path: Path to the Gold windows table.

    Returns:
        The companion rate-table path.
    """
    return f"{gold_path}_rate"


def _load(spark: SparkSession, path: str) -> DataFrame | None:
    """Load a Delta table, returning ``None`` if it does not exist yet.

    Args:
        spark: Active SparkSession.
        path: Delta table path.

    Returns:
        The table DataFrame, or ``None`` when absent.
    """
    try:
        if not DeltaTable.isDeltaTable(spark, path):
            return None
        return spark.read.format("delta").load(path)
    except Exception as exc:  # noqa: BLE001
        log.warning("query.load_failed", path=path, error=str(exc))
        return None


def _to_pandas(df: DataFrame) -> pd.DataFrame:
    """Collect a Spark DataFrame to pandas through Arrow.

    PySpark 3.4's non-Arrow ``toPandas`` casts timestamps to a unit-less
    ``datetime64``, which pandas 2 rejects; the Arrow path converts them
    correctly.

    Args:
        df: The Spark DataFrame to collect.

    Returns:
        The equivalent pandas DataFrame.
    """
    df.sparkSession.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    return df.toPandas()


def _tumbling(df: DataFrame) -> DataFrame:
    """Keep only minute-aligned windows to avoid overlap double counting.

    Args:
        df: The Gold windows DataFrame.

    Returns:
        The DataFrame filtered to windows starting on a whole minute.
    """
    return df.where(F.second("window_start") == 0)


def get_kpi_summary(spark: SparkSession, gold_path: str) -> dict[str, float]:
    """Return headline KPIs aggregated across all tumbling windows.

    Args:
        spark: Active SparkSession.
        gold_path: Gold windows table path.

    Returns:
        A dict with ``total_orders``, ``total_revenue``, ``avg_ticket`` and
        ``unique_customers`` (all zero when no data exists yet).
    """
    df = _load(spark, gold_path)
    empty = {
        "total_orders": 0,
        "total_revenue": 0.0,
        "avg_ticket": 0.0,
        "unique_customers": 0,
    }
    if df is None:
        return empty

    row = (
        _tumbling(df)
        .agg(
            F.sum("total_orders").alias("total_orders"),
            F.sum("total_revenue").alias("total_revenue"),
            F.sum("unique_customers").alias("unique_customers"),
        )
        .collect()
    )
    if not row or row[0]["total_orders"] is None:
        return empty

    total_orders = int(row[0]["total_orders"] or 0)
    total_revenue = float(row[0]["total_revenue"] or 0.0)
    return {
        "total_orders": total_orders,
        "total_revenue": round(total_revenue, 2),
        "avg_ticket": round(total_revenue / total_orders, 2) if total_orders else 0.0,
        "unique_customers": int(row[0]["unique_customers"] or 0),
    }


def get_revenue_by_state(
    spark: SparkSession, gold_path: str, limit: int = 10
) -> pd.DataFrame:
    """Return total revenue per customer state (top ``limit``).

    Args:
        spark: Active SparkSession.
        gold_path: Gold windows table path.
        limit: Number of top states to return.

    Returns:
        A pandas DataFrame with ``customer_state`` and ``total_revenue``.
    """
    df = _load(spark, gold_path)
    if df is None:
        return _EMPTY
    return _to_pandas(
        _tumbling(df)
        .groupBy("customer_state")
        .agg(F.round(F.sum("total_revenue"), 2).alias("total_revenue"))
        .orderBy(F.desc("total_revenue"))
        .limit(limit)
    )


def get_top_categories(
    spark: SparkSession, gold_path: str, limit: int = 10
) -> pd.DataFrame:
    """Return the busiest product categories by order count (top ``limit``).

    Args:
        spark: Active SparkSession.
        gold_path: Gold windows table path.
        limit: Number of top categories to return.

    Returns:
        A pandas DataFrame with ``product_category`` and ``total_orders``.
    """
    df = _load(spark, gold_path)
    if df is None:
        return _EMPTY
    return _to_pandas(
        _tumbling(df)
        .groupBy("product_category")
        .agg(F.sum("total_orders").alias("total_orders"))
        .orderBy(F.desc("total_orders"))
        .limit(limit)
    )


def get_orders_timeseries(
    spark: SparkSession, gold_path: str, minutes: int = 30
) -> pd.DataFrame:
    """Return the orders-per-minute time series from the global rate table.

    Args:
        spark: Active SparkSession.
        gold_path: Gold windows table path (the rate path is derived from it).
        minutes: Look-back window in minutes.

    Returns:
        A pandas DataFrame with ``window_start``, ``orders_per_minute`` and
        ``revenue_rate`` sorted ascending by time.
    """
    df = _load(spark, _rate_path(gold_path))
    if df is None:
        return _EMPTY
    cutoff = F.current_timestamp() - F.expr(f"INTERVAL {minutes} MINUTES")
    return _to_pandas(
        df.where(F.col("window_start") >= cutoff)
        .select("window_start", "orders_per_minute", "revenue_rate")
        .orderBy("window_start")
    )


def get_recent_windows(
    spark: SparkSession, gold_path: str, limit: int = 20
) -> pd.DataFrame:
    """Return the most recent Gold windows.

    Args:
        spark: Active SparkSession.
        gold_path: Gold windows table path.
        limit: Number of recent windows to return.

    Returns:
        A pandas DataFrame with the latest window rows.
    """
    df = _load(spark, gold_path)
    if df is None:
        return _EMPTY
    return _to_pandas(
        df.select(
            "window_start",
            "customer_state",
            "product_category",
            "total_orders",
            "total_revenue",
        )
        .orderBy(F.desc("window_start"))
        .limit(limit)
    )
