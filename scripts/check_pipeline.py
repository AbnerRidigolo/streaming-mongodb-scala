"""Health check for every component of the streaming pipeline.

Prints a status table covering Kafka + Schema Registry, the Spark UI of the
pipeline driver, the Delta tables (Bronze/Silver/Gold), the Streamlit dashboard
and Prometheus.
Exits non-zero if any *core* component (Kafka, Schema Registry) is down.
"""

from __future__ import annotations

import os
from pathlib import Path

import requests
import structlog

log = structlog.get_logger("check_pipeline")

_TIMEOUT = 5


def _check_kafka(bootstrap: str) -> tuple[bool, str]:
    """Check Kafka connectivity and required topics.

    Args:
        bootstrap: Kafka bootstrap servers.

    Returns:
        ``(ok, detail)``.
    """
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": bootstrap})
        topics = set(admin.list_topics(timeout=_TIMEOUT).topics)
        wanted = {
            os.environ.get("TOPIC_ORDERS_RAW", "orders-raw"),
            os.environ.get("TOPIC_PAYMENTS_RAW", "payments-raw"),
            os.environ.get("TOPIC_DELIVERY", "delivery-events"),
        }
        missing = wanted - topics
        if missing:
            return False, f"missing topics: {sorted(missing)}"
        return True, f"{len(topics)} topics present"
    except Exception as exc:  # noqa: BLE001
        return False, f"unreachable: {exc}"


def _check_http(name: str, url: str) -> tuple[bool, str]:
    """Check that an HTTP endpoint responds.

    Args:
        name: Component name (for the detail string).
        url: URL to probe.

    Returns:
        ``(ok, detail)``.
    """
    try:
        resp = requests.get(url, timeout=_TIMEOUT)
        return resp.ok, f"HTTP {resp.status_code}"
    except Exception as exc:  # noqa: BLE001
        return False, f"unreachable: {exc}"


def _check_delta(path: str) -> tuple[bool, str]:
    """Check a Delta table's presence and approximate size.

    Args:
        path: Delta table path.

    Returns:
        ``(ok, detail)`` where detail reports the number of data files.
    """
    table = Path(path)
    if not (table / "_delta_log").exists():
        return False, "not created yet"
    data_files = list(table.rglob("*.parquet"))
    return True, f"{len(data_files)} data files"


def main() -> int:
    """Run every check, print the report and return a process exit code."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    schema_url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    spark_ui = os.environ.get("SPARK_UI", "http://localhost:4040")
    dashboard_port = os.environ.get("DASHBOARD_PORT", "8501")
    prom_port = os.environ.get("PROMETHEUS_PORT", "9090")

    checks: list[tuple[str, bool, str]] = []

    ok, detail = _check_kafka(bootstrap)
    checks.append(("Kafka", ok, detail))
    core_ok = ok

    ok, detail = _check_http("Schema Registry", f"{schema_url}/subjects")
    checks.append(("Schema Registry", ok, detail))
    core_ok = core_ok and ok

    checks.append(("Spark UI", *_check_http("Spark", spark_ui)))
    checks.append(
        (
            "Bronze Delta",
            *_check_delta(os.environ.get("BRONZE_PATH", "data/bronze/orders")),
        )
    )
    checks.append(
        (
            "Silver Delta",
            *_check_delta(os.environ.get("SILVER_PATH", "data/silver/orders_enriched")),
        )
    )
    checks.append(
        (
            "Gold Delta",
            *_check_delta(os.environ.get("GOLD_PATH", "data/gold/orders_agg")),
        )
    )
    checks.append(
        (
            "Dashboard",
            *_check_http(
                "Dashboard", f"http://localhost:{dashboard_port}/_stcore/health"
            ),
        )
    )
    checks.append(
        (
            "Prometheus",
            *_check_http("Prometheus", f"http://localhost:{prom_port}/-/healthy"),
        )
    )

    print("\n  Component            Status   Detail")
    print("  " + "-" * 56)
    for name, ok, detail in checks:
        mark = "✓" if ok else "✗"
        print(f"  {name:<20} {mark:<8} {detail}")
    print()

    log.info(
        "check_pipeline.done",
        core_ok=core_ok,
        passed=sum(1 for _, ok, _ in checks if ok),
        total=len(checks),
    )
    return 0 if core_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
