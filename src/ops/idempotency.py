"""Prove the pipeline is re-runnable, in the job itself.

"What happens if the job runs twice?" is a top interview question and the answer
should be a test, not a promise. This task fingerprints the gold layer after
every run and fails the job if a re-run of the same `run_date` produced
different output.

A fingerprint is row count plus the sums of the additive measures, per table.
That catches the failure modes that matter here:

  * facts appended instead of replaced        -> row count moves
  * SCD2 opening a version on an unchanged row -> dim_customer count moves
  * a join fanning out                        -> count and sums both move
  * double-counted revenue                    -> sums move while counts hold

What it does NOT catch: two runs that are wrong in exactly the same way. It is a
regression guard, not a correctness proof -- the reconciliation checks in
sql/views/ cover correctness.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.config import SCHEMA_GOLD, SCHEMA_OPS, fqn

def fingerprint_table() -> str:
    return fqn(SCHEMA_OPS, "gold_fingerprints")

# Table -> additive measures to sum. Empty tuple means count only.
FINGERPRINT_SPEC: dict[str, tuple[str, ...]] = {
    "fact_order": ("order_total", "items_total", "freight_total", "items_count"),
    "fact_order_item": ("item_revenue", "item_price", "freight_value"),
    "fact_payment": ("payment_value",),
    "dim_customer": (),
    "dim_product": (),
    "dim_seller": (),
    "dim_date": (),
}


def _ddl() -> str:
    """Built at call time so it targets the run's catalog, not the default."""
    return f"""
    CREATE TABLE IF NOT EXISTS {fingerprint_table()} (
      run_id       STRING,
      run_date     DATE,
      table_name   STRING,
      row_count    BIGINT,
      measure_sums MAP<STRING, DECIMAL(20,2)>,
      captured_at  TIMESTAMP
    )
    COMMENT 'Gold-layer fingerprint per run. Grain: (run_id, table_name).
             Used by the idempotency_check task to detect non-deterministic reruns.'
    """


def capture(spark: SparkSession, run_id: str, run_date: str) -> dict[str, dict]:
    """Fingerprint every gold table that exists and record it."""
    spark.sql(_ddl())
    rows, result = [], {}

    for table, measures in FINGERPRINT_SPEC.items():
        full = fqn(SCHEMA_GOLD, table)
        if not spark.catalog.tableExists(full):
            continue

        aggs = [F.count(F.lit(1)).alias("row_count")]
        aggs += [
            F.coalesce(F.sum(F.col(m).cast("decimal(20,2)")), F.lit(0)).alias(m)
            for m in measures
        ]
        agg = spark.table(full).agg(*aggs).collect()[0]

        sums = {m: agg[m] for m in measures}
        rows.append((run_id, run_date, table, int(agg["row_count"]), sums))
        result[table] = {"row_count": int(agg["row_count"]), "measure_sums": sums}

    if rows:
        spark.createDataFrame(
            rows,
            "run_id string, run_date string, table_name string, row_count long, "
            "measure_sums map<string,decimal(20,2)>",
        ).withColumn("run_date", F.to_date("run_date")).withColumn(
            "captured_at", F.current_timestamp()
        ).write.mode("append").saveAsTable(fingerprint_table())

    return result


def check(spark: SparkSession, run_id: str, run_date: str, strict: bool = True) -> list[str]:
    """Compare this run's fingerprint against the previous run of the same date.

    Returns a list of human-readable differences. With strict=True, a non-empty
    list raises -- the job must go red, because a pipeline that quietly stopped
    being idempotent is exactly the thing this guards.

    The first run for a given run_date has nothing to compare against and passes.
    """
    current = capture(spark, run_id, run_date)

    previous_run = spark.sql(f"""
        SELECT run_id
        FROM {fingerprint_table()}
        WHERE run_date = date('{run_date}') AND run_id <> '{run_id}'
        ORDER BY captured_at DESC
        LIMIT 1
    """).collect()

    if not previous_run:
        print(f"idempotency: first run for {run_date}; baseline recorded, nothing to compare")
        return []

    prev_id = previous_run[0]["run_id"]
    prev_rows = spark.sql(f"""
        SELECT table_name, row_count, measure_sums
        FROM {fingerprint_table()}
        WHERE run_id = '{prev_id}'
    """).collect()
    previous = {
        r["table_name"]: {"row_count": r["row_count"], "measure_sums": r["measure_sums"]}
        for r in prev_rows
    }

    diffs: list[str] = []
    for table, now in current.items():
        was = previous.get(table)
        if was is None:
            diffs.append(f"{table}: absent in run {prev_id}, present now")
            continue

        if was["row_count"] != now["row_count"]:
            diffs.append(
                f"{table}.row_count: {was['row_count']} -> {now['row_count']} "
                f"(delta {now['row_count'] - was['row_count']:+d})"
            )
        for measure, value in now["measure_sums"].items():
            before = (was["measure_sums"] or {}).get(measure)
            if before is not None and before != value:
                diffs.append(f"{table}.{measure}: {before} -> {value}")

    for table in previous.keys() - current.keys():
        diffs.append(f"{table}: present in run {prev_id}, absent now")

    if diffs:
        message = "\n  ".join(["Gold layer changed across identical runs:"] + diffs)
        if strict:
            raise AssertionError(message)
        print(message)
    else:
        print(
            f"idempotency: PASS -- {len(current)} gold tables identical to run {prev_id}"
        )

    return diffs
