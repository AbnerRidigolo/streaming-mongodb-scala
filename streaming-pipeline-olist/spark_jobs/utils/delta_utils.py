"""Delta Lake helper utilities: merge/upsert, compaction, vacuum and stats.

These helpers centralize the Delta maintenance operations used across the
ingestion, enrichment and aggregation jobs, returning structured metrics that
are logged via :mod:`structlog` and surfaced to the dashboard.
"""

from __future__ import annotations

from typing import Any

import structlog
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

log = structlog.get_logger("delta_utils")


def _table_exists(spark: SparkSession, path: str) -> bool:
    """Return whether a Delta table already exists at ``path``.

    Args:
        spark: Active SparkSession.
        path: Filesystem/object-store path of the Delta table.

    Returns:
        ``True`` if a Delta table is present at the path.
    """
    return DeltaTable.isDeltaTable(spark, path)


def upsert_delta(
    spark: SparkSession,
    df: DataFrame,
    path: str,
    merge_condition: str,
    partition_cols: list[str] | None = None,
) -> dict[str, Any]:
    """Idempotently upsert a DataFrame into a Delta table via MERGE.

    On first call the table is created (partitioned by ``partition_cols``).
    Subsequent calls MERGE on ``merge_condition`` using source alias ``s`` and
    target alias ``t``.

    Args:
        spark: Active SparkSession.
        df: Source DataFrame to upsert.
        path: Target Delta table path.
        merge_condition: SQL predicate joining target ``t`` and source ``s``
            (e.g. ``"t.event_id = s.event_id"``).
        partition_cols: Partition columns used only when creating the table.

    Returns:
        A dict of merge metrics: ``rows_inserted``, ``rows_updated``,
        ``rows_deleted`` and ``created`` (``True`` when the table was created).
    """
    if not _table_exists(spark, path):
        writer = df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)
        log.info("delta.created", path=path, rows=df.count())
        return {
            "created": True,
            "rows_inserted": df.count(),
            "rows_updated": 0,
            "rows_deleted": 0,
        }

    target = DeltaTable.forPath(spark, path)
    (
        target.alias("t")
        .merge(df.alias("s"), merge_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    metrics = _last_operation_metrics(spark, path)
    stats = {
        "created": False,
        "rows_inserted": int(metrics.get("numTargetRowsInserted", 0)),
        "rows_updated": int(metrics.get("numTargetRowsUpdated", 0)),
        "rows_deleted": int(metrics.get("numTargetRowsDeleted", 0)),
    }
    log.info("delta.merged", path=path, **stats)
    return stats


def _last_operation_metrics(spark: SparkSession, path: str) -> dict[str, Any]:
    """Return the ``operationMetrics`` map of the most recent table operation.

    Args:
        spark: Active SparkSession.
        path: Delta table path.

    Returns:
        The operation metrics as a ``dict`` (empty if unavailable).
    """
    history = (
        DeltaTable.forPath(spark, path).history(1).select("operationMetrics").collect()
    )
    if not history or history[0][0] is None:
        return {}
    return dict(history[0][0])


def compact_delta_table(
    spark: SparkSession,
    path: str,
    target_file_size_mb: int = 128,
    zorder_by: list[str] | None = None,
) -> dict[str, Any]:
    """Run Delta ``OPTIMIZE`` (with optional ``ZORDER``) to compact small files.

    Args:
        spark: Active SparkSession.
        path: Delta table path.
        target_file_size_mb: Desired output file size in megabytes.
        zorder_by: Optional columns to Z-order by for data skipping.

    Returns:
        The OPTIMIZE operation metrics (e.g. ``numFilesAdded``,
        ``numFilesRemoved``).
    """
    spark.conf.set(
        "spark.databricks.delta.optimize.maxFileSize",
        str(target_file_size_mb * 1024 * 1024),
    )
    sql = f"OPTIMIZE delta.`{path}`"
    if zorder_by:
        sql += f" ZORDER BY ({', '.join(zorder_by)})"
    spark.sql(sql)
    metrics = _last_operation_metrics(spark, path)
    log.info(
        "delta.compacted",
        path=path,
        target_file_size_mb=target_file_size_mb,
        zorder_by=zorder_by,
        files_added=metrics.get("numFilesAdded"),
        files_removed=metrics.get("numFilesRemoved"),
    )
    return metrics


def vacuum_delta_table(
    spark: SparkSession,
    path: str,
    retention_hours: int = 168,
) -> None:
    """Remove files no longer referenced by the table older than the retention.

    Args:
        spark: Active SparkSession.
        path: Delta table path.
        retention_hours: Retention window in hours (default 7 days). Values
            below 168 temporarily disable the retention-duration safety check.
    """
    if retention_hours < 168:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM delta.`{path}` RETAIN {retention_hours} HOURS")
    log.info("delta.vacuumed", path=path, retention_hours=retention_hours)


def get_table_stats(spark: SparkSession, path: str) -> dict[str, Any]:
    """Return summary statistics for a Delta table.

    Args:
        spark: Active SparkSession.
        path: Delta table path.

    Returns:
        A dict with ``num_files``, ``size_bytes``, ``num_rows`` and
        ``last_modified``. All values are ``0``/``None`` if the table is absent.
    """
    if not _table_exists(spark, path):
        return {
            "num_files": 0,
            "size_bytes": 0,
            "num_rows": 0,
            "last_modified": None,
        }

    detail = DeltaTable.forPath(spark, path).detail().collect()[0]
    num_rows = spark.read.format("delta").load(path).count()
    stats = {
        "num_files": int(detail["numFiles"]),
        "size_bytes": int(detail["sizeInBytes"]),
        "num_rows": num_rows,
        "last_modified": str(detail["lastModified"]),
    }
    log.info("delta.stats", path=path, **stats)
    return stats
