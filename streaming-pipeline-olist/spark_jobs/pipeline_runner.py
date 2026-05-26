"""Pipeline runner: supervises ingestion, enrichment and aggregation jobs.

Starts the three streaming jobs in dependency order (Bronze → Silver → Gold)
using a thread pool, monitors query liveness, restarts failed jobs up to a
bounded number of attempts and shuts everything down gracefully on SIGINT /
SIGTERM.
"""

from __future__ import annotations

import os
import signal
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

import structlog
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from aggregation_job import run_aggregation_job
from enrichment_job import run_enrichment_job
from ingestion_job import run_ingestion_job
from spark_session import get_streaming_session

log = structlog.get_logger("pipeline_runner")

MAX_RESTARTS = 3
HEALTH_CHECK_INTERVAL = 30
STATUS_LOG_INTERVAL = 60
DEPENDENCY_TIMEOUT = 300


class PipelineRunner:
    """Supervises the three streaming jobs that make up the pipeline."""

    def __init__(self, spark: SparkSession) -> None:
        """Initialize the runner.

        Args:
            spark: Shared SparkSession used by every job.
        """
        self.spark = spark
        self._shutdown = threading.Event()
        self._lock = threading.Lock()
        self._active: dict[str, list[Any]] = {}
        self._restarts: dict[str, int] = {}

    # ----------------------------------------------------------- dependency wait

    def _wait_for_table(self, path: str, timeout: int = DEPENDENCY_TIMEOUT) -> bool:
        """Block until a Delta table exists at ``path`` or the timeout elapses.

        Args:
            path: Delta table path to wait for.
            timeout: Maximum seconds to wait.

        Returns:
            ``True`` if the table appeared, ``False`` on timeout/shutdown.
        """
        deadline = time.monotonic() + timeout
        while not self._shutdown.is_set() and time.monotonic() < deadline:
            try:
                if DeltaTable.isDeltaTable(self.spark, path):
                    return True
            except Exception:  # noqa: BLE001 - path not ready yet
                pass
            self._shutdown.wait(3)
        return False

    # ------------------------------------------------------------- supervision

    def _supervise(
        self,
        name: str,
        start_fn: Callable[[SparkSession], Any],
        depends_on: str | None,
    ) -> None:
        """Start a job and keep it alive, restarting on failure.

        Args:
            name: Logical job name (for logs/status).
            start_fn: Callable that starts the job and returns a query (or list).
            depends_on: Optional upstream Delta path that must exist first.
        """
        self._restarts[name] = 0
        while not self._shutdown.is_set():
            if depends_on and not self._wait_for_table(depends_on):
                if self._shutdown.is_set():
                    return
                log.error("job.dependency.timeout", job=name, path=depends_on)
                return
            try:
                result = start_fn(self.spark)
                queries = result if isinstance(result, list) else [result]
                with self._lock:
                    self._active[name] = queries
                log.info("job.started", job=name, queries=len(queries))
                self._monitor_queries(name, queries)
            except Exception as exc:  # noqa: BLE001
                log.error("job.crashed", job=name, error=str(exc))

            if self._shutdown.is_set():
                return
            self._restarts[name] += 1
            if self._restarts[name] > MAX_RESTARTS:
                log.error("job.restart.exhausted", job=name, attempts=MAX_RESTARTS)
                return
            log.warning(
                "job.restarting", job=name, attempt=self._restarts[name]
            )
            self._shutdown.wait(5)

    def _monitor_queries(self, name: str, queries: list[Any]) -> None:
        """Poll the job's queries until one dies or shutdown is requested.

        Args:
            name: Job name.
            queries: The job's active streaming queries.
        """
        while not self._shutdown.is_set():
            for query in queries:
                if not query.isActive:
                    exc = query.exception()
                    log.error("query.inactive", job=name, error=str(exc))
                    return
            self._shutdown.wait(5)
        # Graceful shutdown requested — stop the queries.
        for query in queries:
            try:
                query.stop()
            except Exception:  # noqa: BLE001
                pass

    # --------------------------------------------------------------- monitoring

    def _status_loop(self) -> None:
        """Emit periodic health-check (30s) and status (60s) log lines."""
        last_status = 0.0
        while not self._shutdown.is_set():
            with self._lock:
                snapshot = {
                    name: [q.isActive for q in queries]
                    for name, queries in self._active.items()
                }
            healthy = all(all(states) for states in snapshot.values()) and snapshot
            log.info(
                "pipeline.healthcheck",
                healthy=bool(healthy),
                jobs={n: sum(s) for n, s in snapshot.items()},
            )
            now = time.monotonic()
            if now - last_status >= STATUS_LOG_INTERVAL:
                log.info(
                    "pipeline.status",
                    jobs={
                        name: ("active" if all(states) else "degraded")
                        for name, states in snapshot.items()
                    },
                    restarts=dict(self._restarts),
                )
                last_status = now
            self._shutdown.wait(HEALTH_CHECK_INTERVAL)

    # ----------------------------------------------------------------- lifecycle

    def run(self) -> None:
        """Start all jobs + the status loop and block until shutdown."""
        bronze = os.environ.get("BRONZE_PATH", "data/bronze/orders")
        silver = os.environ.get("SILVER_PATH", "data/silver/orders_enriched")

        status_thread = threading.Thread(
            target=self._status_loop, name="status-loop", daemon=True
        )
        status_thread.start()

        with ThreadPoolExecutor(max_workers=3, thread_name_prefix="job") as pool:
            pool.submit(self._supervise, "ingestion", run_ingestion_job, None)
            pool.submit(self._supervise, "enrichment", run_enrichment_job, bronze)
            pool.submit(self._supervise, "aggregation", run_aggregation_job, silver)
            # Block the main thread until a shutdown signal arrives.
            while not self._shutdown.is_set():
                self._shutdown.wait(1)
            log.info("pipeline.shutdown.begin")

        log.info("pipeline.shutdown.complete")

    def request_shutdown(self, *_: Any) -> None:
        """Signal handler that triggers a graceful shutdown."""
        log.warning("pipeline.signal.received")
        self._shutdown.set()


def main() -> None:
    """Entry point: build the SparkSession and run the supervised pipeline."""
    spark = get_streaming_session(
        "olist-pipeline", os.environ.get("SPARK_ENV", "cluster")
    )
    runner = PipelineRunner(spark)
    signal.signal(signal.SIGINT, runner.request_shutdown)
    signal.signal(signal.SIGTERM, runner.request_shutdown)
    runner.run()
    spark.stop()


if __name__ == "__main__":
    main()
