"""Unit tests for :mod:`spark_jobs.utils.delta_utils`."""

from __future__ import annotations

from pyspark.sql import SparkSession

from utils.delta_utils import compact_delta_table, get_table_stats, upsert_delta

MERGE_ON = "t.id = s.id"


def _df(spark: SparkSession, rows: list[tuple[int, str]]):
    """Build a tiny ``(id, value)`` DataFrame."""
    return spark.createDataFrame(rows, schema=["id", "value"])


def test_upsert_inserts_new_records(spark: SparkSession, tmp_delta_path: str) -> None:
    """New keys are inserted by the MERGE path."""
    upsert_delta(spark, _df(spark, [(1, "a"), (2, "b")]), tmp_delta_path, MERGE_ON)
    stats = upsert_delta(
        spark, _df(spark, [(3, "c"), (4, "d")]), tmp_delta_path, MERGE_ON
    )

    result = spark.read.format("delta").load(tmp_delta_path)
    assert result.count() == 4
    assert stats["rows_inserted"] == 2


def test_upsert_updates_existing_records(
    spark: SparkSession, tmp_delta_path: str
) -> None:
    """Matching keys are updated, not duplicated."""
    upsert_delta(spark, _df(spark, [(1, "a"), (2, "b")]), tmp_delta_path, MERGE_ON)
    stats = upsert_delta(spark, _df(spark, [(1, "A")]), tmp_delta_path, MERGE_ON)

    result = spark.read.format("delta").load(tmp_delta_path)
    assert result.count() == 2
    assert result.where("id = 1").collect()[0]["value"] == "A"
    assert stats["rows_updated"] == 1


def test_upsert_is_idempotent(spark: SparkSession, tmp_delta_path: str) -> None:
    """Running the same upsert twice yields the same table state."""
    upsert_delta(spark, _df(spark, [(1, "a"), (2, "b")]), tmp_delta_path, MERGE_ON)
    payload = _df(spark, [(1, "a"), (2, "b")])
    upsert_delta(spark, payload, tmp_delta_path, MERGE_ON)
    upsert_delta(spark, payload, tmp_delta_path, MERGE_ON)

    result = spark.read.format("delta").load(tmp_delta_path)
    assert result.count() == 2
    assert {r["value"] for r in result.collect()} == {"a", "b"}


def test_upsert_returns_merge_stats(spark: SparkSession, tmp_delta_path: str) -> None:
    """The merge returns a stats dict with the expected keys."""
    upsert_delta(spark, _df(spark, [(1, "a")]), tmp_delta_path, MERGE_ON)
    stats = upsert_delta(
        spark, _df(spark, [(1, "b"), (2, "c")]), tmp_delta_path, MERGE_ON
    )

    assert set(stats) == {"created", "rows_inserted", "rows_updated", "rows_deleted"}
    assert stats["created"] is False
    assert stats["rows_inserted"] == 1
    assert stats["rows_updated"] == 1


def test_compact_reduces_file_count(spark: SparkSession, tmp_delta_path: str) -> None:
    """OPTIMIZE collapses many small files into fewer."""
    for i in range(6):
        (
            _df(spark, [(i, f"v{i}")])
            .write.format("delta")
            .mode("append")
            .save(tmp_delta_path)
        )
    before = get_table_stats(spark, tmp_delta_path)["num_files"]
    compact_delta_table(spark, tmp_delta_path, target_file_size_mb=128)
    after = get_table_stats(spark, tmp_delta_path)["num_files"]

    assert before > 1
    assert after <= before


def test_get_table_stats_returns_correct_schema(
    spark: SparkSession, tmp_delta_path: str
) -> None:
    """get_table_stats returns the documented keys with sane values."""
    upsert_delta(
        spark, _df(spark, [(1, "a"), (2, "b"), (3, "c")]), tmp_delta_path, MERGE_ON
    )
    stats = get_table_stats(spark, tmp_delta_path)

    assert set(stats) == {"num_files", "size_bytes", "num_rows", "last_modified"}
    assert stats["num_rows"] == 3
    assert stats["num_files"] >= 1
    assert stats["size_bytes"] > 0
