"""Create the pipeline's Kafka topics with their required configurations.

Idempotent: topics that already exist are skipped. Run via ``make topics`` or
``python scripts/create_topics.py``.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Make the project root importable when run as a standalone script.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import structlog  # noqa: E402

from spark_jobs.utils.kafka_utils import create_topic  # noqa: E402

log = structlog.get_logger("create_topics")

# (topic, partitions, replication, config) — config matches the project spec.
TOPIC_SPECS: list[tuple[str, int, int, dict[str, str]]] = [
    (
        os.environ.get("TOPIC_ORDERS_RAW", "orders-raw"),
        3,
        1,
        {"retention.ms": "86400000", "max.message.bytes": "10485760"},
    ),
    (
        os.environ.get("TOPIC_PAYMENTS_RAW", "payments-raw"),
        3,
        1,
        {"retention.ms": "86400000"},
    ),
    (
        os.environ.get("TOPIC_DELIVERY", "delivery-events"),
        2,
        1,
        {"retention.ms": "43200000"},
    ),
    (
        os.environ.get("TOPIC_ORDERS_ENRICHED", "orders-enriched"),
        6,
        1,
        {"cleanup.policy": "compact"},
    ),
]


def main() -> None:
    """Create every topic in :data:`TOPIC_SPECS`."""
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    log.info("create_topics.start", bootstrap_servers=bootstrap)

    created, skipped = 0, 0
    for topic, partitions, replication, config in TOPIC_SPECS:
        was_created = create_topic(
            bootstrap_servers=bootstrap,
            topic_name=topic,
            partitions=partitions,
            replication_factor=replication,
            config=config,
        )
        created += int(was_created)
        skipped += int(not was_created)

    log.info("create_topics.done", created=created, skipped=skipped)


if __name__ == "__main__":
    main()
