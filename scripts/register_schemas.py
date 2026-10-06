"""Register the Avro schemas with the Confluent Schema Registry (idempotent).

Registers ``order_event.avsc``, ``payment_event.avsc`` and
``delivery_event.avsc`` under the
topic-name-strategy subjects (``<topic>-value``). If an identical schema is
already registered, Schema Registry returns the existing id, so re-running is
safe. Prints the subject and schema id for each schema.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import structlog  # noqa: E402
from confluent_kafka.schema_registry import Schema, SchemaRegistryClient  # noqa: E402

log = structlog.get_logger("register_schemas")

SCHEMAS_DIR = Path(__file__).resolve().parent.parent / "producers" / "schemas"

# (avsc filename, subject) pairs.
SCHEMA_SPECS: list[tuple[str, str]] = [
    ("order_event.avsc", f"{os.environ.get('TOPIC_ORDERS_RAW', 'orders-raw')}-value"),
    (
        "payment_event.avsc",
        f"{os.environ.get('TOPIC_PAYMENTS_RAW', 'payments-raw')}-value",
    ),
    (
        "delivery_event.avsc",
        f"{os.environ.get('TOPIC_DELIVERY', 'delivery-events')}-value",
    ),
]


def _already_registered(client: SchemaRegistryClient, subject: str) -> bool:
    """Return whether a subject already has at least one registered version.

    Args:
        client: Schema Registry client.
        subject: Subject name to check.

    Returns:
        ``True`` if the subject exists.
    """
    try:
        return subject in client.get_subjects()
    except Exception:  # noqa: BLE001
        return False


def main() -> None:
    """Register every schema in :data:`SCHEMA_SPECS`."""
    url = os.environ.get("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    client = SchemaRegistryClient({"url": url})
    log.info("register_schemas.start", schema_registry_url=url)

    for filename, subject in SCHEMA_SPECS:
        schema_str = (SCHEMAS_DIR / filename).read_text(encoding="utf-8")
        existed = _already_registered(client, subject)
        schema_id = client.register_schema(subject, Schema(schema_str, "AVRO"))
        log.info(
            "schema.registered",
            file=filename,
            subject=subject,
            schema_id=schema_id,
            pre_existing=existed,
        )
        print(
            f"  {subject:<24} -> schema id {schema_id} "
            f"({'existing' if existed else 'new'})"
        )

    log.info("register_schemas.done", count=len(SCHEMA_SPECS))


if __name__ == "__main__":
    main()
