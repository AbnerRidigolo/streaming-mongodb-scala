"""SparkSession factory for the Olist streaming pipeline.

Centralizes the Spark configuration shared by every streaming job:

* Delta Lake SQL extensions + catalog,
* Kafka, Avro and Delta JAR packages,
* Adaptive Query Execution + auto-compaction tuning,
* a sane shuffle-partition count for a single-node local environment,
* a configurable log level (defaults to ``WARN`` to silence INFO noise).
"""

from __future__ import annotations

import os
from typing import Final

import structlog
from pyspark.sql import SparkSession

log = structlog.get_logger("spark_session")

# JAR packages required by the streaming jobs. ``spark-avro`` provides the
# ``from_avro`` function used to decode Confluent-Avro payloads (the 5-byte
# Confluent header is stripped in ingestion_job, with a fixed reader schema).
_DEFAULT_PACKAGES: Final[str] = ",".join(
    [
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
        "org.apache.spark:spark-avro_2.12:3.4.1",
        "io.delta:delta-core_2.12:2.4.0",
    ]
)


def get_streaming_session(
    app_name: str,
    environment: str = "local",
) -> SparkSession:
    """Build (or fetch) a configured :class:`SparkSession` for streaming.

    Args:
        app_name: Spark application name shown in the UI.
        environment: ``"local"`` runs against ``local[*]``; any other value
            uses the ``SPARK_MASTER`` environment variable (e.g. a standalone
            cluster) so the same factory works inside Docker.

    Returns:
        A configured :class:`SparkSession` with Delta + Kafka + Avro support.
    """
    master = (
        "local[*]"
        if environment == "local"
        else os.environ.get("SPARK_MASTER", "local[*]")
    )
    checkpoint = os.environ.get("CHECKPOINT_LOCATION", "/tmp/streaming-checkpoints")
    packages = os.environ.get("SPARK_JARS_PACKAGES", _DEFAULT_PACKAGES)
    log_level = os.environ.get("SPARK_LOG_LEVEL", "WARN")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # ---- Delta Lake ----
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ---- Adaptive Query Execution ----
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # ---- Delta write optimizations ----
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        # ---- Local-friendly shuffle sizing ----
        .config("spark.sql.shuffle.partitions", "8")
        # ---- Streaming checkpoint root ----
        .config("spark.sql.streaming.checkpointLocation", checkpoint)
        .config("spark.sql.session.timeZone", "UTC")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(log_level)
    log.info(
        "spark_session.created",
        app_name=app_name,
        environment=environment,
        master=master,
        checkpoint=checkpoint,
        log_level=log_level,
    )
    return spark


def get_test_session(app_name: str = "olist-tests") -> SparkSession:
    """Build a lightweight local :class:`SparkSession` for tests (Delta, no Kafka).

    Args:
        app_name: Spark application name.

    Returns:
        A local ``SparkSession`` configured with Delta and spark-avro.
    """
    from delta import configure_spark_with_delta_pip

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
    )
    # spark-avro lets tests exercise the from_avro decoding path.
    spark = configure_spark_with_delta_pip(
        builder, extra_packages=["org.apache.spark:spark-avro_2.12:3.4.1"]
    ).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
