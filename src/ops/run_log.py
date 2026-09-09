"""Pipeline observability: one row per task per run.

Answers "how do you know the pipeline worked?" with a table instead of a shrug.
Deliberately tiny -- a context manager that stamps start/end/status and row
counts into ops.pipeline_runs, and a helper for DQ results.
"""

from __future__ import annotations

import time
import traceback
from contextlib import contextmanager

from pyspark.sql import functions as F

from src.config import SCHEMA_OPS, fqn

def runs_table() -> str:
    return fqn(SCHEMA_OPS, "pipeline_runs")


def dq_table() -> str:
    return fqn(SCHEMA_OPS, "dq_results")


def _ddl() -> dict[str, str]:
    """DDL built at call time so it targets the run's catalog, not the default."""
    return {
        runs_table(): f"""
            CREATE TABLE IF NOT EXISTS {runs_table()} (
              run_id            STRING  COMMENT 'Databricks job run id, or a local uuid',
              task_name         STRING,
              run_date          DATE    COMMENT 'Logical date the run processes, not wall clock',
              started_at        TIMESTAMP,
              ended_at          TIMESTAMP,
              duration_seconds  DOUBLE,
              status            STRING  COMMENT 'succeeded | failed',
              rows_read         BIGINT,
              rows_written      BIGINT,
              rows_quarantined  BIGINT,
              error_message     STRING,
              details           MAP<STRING, STRING>
            )
            COMMENT 'One row per pipeline task execution. Grain: (run_id, task_name).'
        """,
        dq_table(): f"""
            CREATE TABLE IF NOT EXISTS {dq_table()} (
              run_id        STRING,
              run_date      DATE,
              table_name    STRING,
              rule_id       STRING,
              description   STRING,
              severity      STRING,
              rows_checked  BIGINT,
              rows_failed   BIGINT,
              failure_rate  DOUBLE,
              evaluated_at  TIMESTAMP
            )
            COMMENT 'Per-run outcome of every data-quality rule. Grain: (run_id, table_name, rule_id).'
        """,
    }


def ensure_tables(spark) -> None:
    for ddl in _ddl().values():
        spark.sql(ddl)


class TaskMetrics:
    """Mutable counters a task fills in as it goes."""

    def __init__(self) -> None:
        self.rows_read = 0
        self.rows_written = 0
        self.rows_quarantined = 0
        self.details: dict[str, str] = {}


@contextmanager
def logged_task(spark, task_name: str, run_id: str, run_date: str):
    """Wrap a pipeline task so it always records an outcome.

    Failures are logged and then re-raised -- the job must still go red. A task
    that fails silently but logs "succeeded" is worse than no logging.
    """
    ensure_tables(spark)
    metrics = TaskMetrics()
    started = time.time()
    status, error = "succeeded", None

    try:
        yield metrics
    except Exception as exc:  # noqa: BLE001 - re-raised below
        status = "failed"
        error = f"{type(exc).__name__}: {exc}\n{traceback.format_exc(limit=5)}"
        raise
    finally:
        ended = time.time()
        row = spark.createDataFrame(
            [
                (
                    run_id,
                    task_name,
                    run_date,
                    float(started),
                    float(ended),
                    round(ended - started, 3),
                    status,
                    int(metrics.rows_read),
                    int(metrics.rows_written),
                    int(metrics.rows_quarantined),
                    error,
                    metrics.details,
                )
            ],
            "run_id string, task_name string, run_date string, started_at double, "
            "ended_at double, duration_seconds double, status string, rows_read long, "
            "rows_written long, rows_quarantined long, error_message string, "
            "details map<string,string>",
        )
        (
            row.withColumn("run_date", F.to_date("run_date"))
            .withColumn("started_at", F.timestamp_seconds("started_at"))
            .withColumn("ended_at", F.timestamp_seconds("ended_at"))
            .write.mode("append")
            .saveAsTable(runs_table())
        )


def record_dq_results(
    spark,
    run_id: str,
    run_date: str,
    table_name: str,
    rules,
    failure_counts: dict[str, int],
    rows_checked: int,
) -> None:
    """Write one row per rule with how many records failed it this run.

    `failure_counts` comes from transforms.count_rule_failures over the FULL
    input, not from the quarantine table -- quarantine holds only reject-severity
    failures, so counting from it would report every `warn` rule as clean.
    """
    ensure_tables(spark)
    if not rules:
        return

    failures = failure_counts

    rows = [
        (
            run_id,
            run_date,
            table_name,
            r.rule_id,
            r.description,
            r.severity,
            int(rows_checked),
            int(failures.get(r.rule_id, 0)),
            round(failures.get(r.rule_id, 0) / rows_checked, 6) if rows_checked else 0.0,
        )
        for r in rules
    ]

    spark.createDataFrame(
        rows,
        "run_id string, run_date string, table_name string, rule_id string, "
        "description string, severity string, rows_checked long, rows_failed long, "
        "failure_rate double",
    ).withColumn("run_date", F.to_date("run_date")).withColumn(
        "evaluated_at", F.current_timestamp()
    ).write.mode("append").saveAsTable(dq_table())
