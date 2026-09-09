"""Silver: type, clean, de-duplicate and validate each bronze table.

Composes the pure functions from transforms.py. This module does the I/O; that
one does the thinking. Keeping the split means the logic that carries the
correctness risk is unit-tested without needing a metastore.

Write strategy is overwrite-per-table from bronze, not append. Silver is a
deterministic function of bronze, so recomputing it is always safe and makes the
whole layer replayable -- which is what guarantee (2) of the idempotency story
rests on.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from src.config import (
    INGEST_TS,
    SCHEMA_BRONZE,
    SCHEMA_SILVER,
    SOURCE_TABLES,
    SourceTable,
    fqn,
)
from src.ops import run_log
from src.silver.dq import rules_for
from src.silver.transforms import (
    aggregate_geolocation,
    count_rule_failures,
    dedupe_by_key,
    normalise_strings,
    parse_timestamps,
    rename_columns,
    split_by_rules,
)

# Numeric columns to cast per table. Bronze is all-string on purpose, so the
# cast happens exactly once, here, where a failure is visible.
NUMERIC_CASTS: dict[str, dict[str, str]] = {
    "order_items": {
        "order_item_id": "int",
        "price": "decimal(10,2)",
        "freight_value": "decimal(10,2)",
    },
    "order_payments": {
        "payment_sequential": "int",
        "payment_installments": "int",
        "payment_value": "decimal(10,2)",
    },
    "order_reviews": {"review_score": "int"},
    "products": {
        "product_name_length": "int",
        "product_description_length": "int",
        "product_photos_qty": "int",
        "product_weight_g": "int",
        "product_length_cm": "int",
        "product_height_cm": "int",
        "product_width_cm": "int",
    },
    "customers": {"customer_zip_code_prefix": "int"},
    "sellers": {"seller_zip_code_prefix": "int"},
    "geolocation": {
        "geolocation_zip_code_prefix": "int",
        "geolocation_lat": "double",
        "geolocation_lng": "double",
    },
}


def cast_numerics(df: DataFrame, table: str) -> DataFrame:
    for col, dtype in NUMERIC_CASTS.get(table, {}).items():
        if col in df.columns:
            df = df.withColumn(col, df[col].cast(dtype))
    return df


def clean_table(
    spark: SparkSession, table: SourceTable
) -> tuple[DataFrame, DataFrame, dict[str, int]]:
    """Bronze -> (clean, quarantined, per-rule failure counts) for one table."""
    df = spark.table(fqn(SCHEMA_BRONZE, table.name))

    df = rename_columns(df, table.rename)  # fix Olist's shipped 'lenght' typos
    df = cast_numerics(df, table.name)
    df = parse_timestamps(df, table.timestamp_cols)
    df = normalise_strings(df, table.string_cols)

    # Tie-break on _ingest_ts: most Olist tables have no updated_at, so the
    # fallback is "the most recently landed copy wins".
    df = dedupe_by_key(
        df, table.business_key, order_by=table.sequence_col, tiebreak=INGEST_TS
    )

    if table.name == "geolocation":
        df = aggregate_geolocation(df)

    rules = rules_for(table.name)
    clean, quarantined = split_by_rules(df, rules)
    # Counted over the full input so `warn` failures are visible too.
    counts = {
        r["rule_id"]: r["rows_failed"]
        for r in count_rule_failures(df, rules).collect()
    }
    return clean, quarantined, counts


def clean_all(
    spark: SparkSession,
    run_id: str,
    run_date: str,
    tables: list[str] | None = None,
) -> dict[str, int]:
    """Build silver tables, quarantine failures, record DQ results.

    `tables` restricts the run to named tables. Used by the DQ probe
    (notebooks/98_dq_probe.py) so it exercises this exact code path rather than
    a reimplementation of it, and useful for re-running a single table after a
    fix without reprocessing all eight.
    """
    written: dict[str, int] = {}
    selected = [t for t in SOURCE_TABLES if tables is None or t.name in tables]
    if tables:
        unknown = set(tables) - {t.name for t in SOURCE_TABLES}
        if unknown:
            raise ValueError(f"unknown table(s): {sorted(unknown)}")

    for table in selected:
        clean, quarantined, failure_counts = clean_table(spark, table)
        rules = rules_for(table.name)

        clean.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
            fqn(SCHEMA_SILVER, table.name)
        )
        n_clean = spark.table(fqn(SCHEMA_SILVER, table.name)).count()

        n_quarantined = 0
        if rules:
            q_table = fqn(SCHEMA_SILVER, f"quarantine_{table.name}")
            quarantined.write.mode("overwrite").option(
                "overwriteSchema", "true"
            ).saveAsTable(q_table)
            n_quarantined = spark.table(q_table).count()

            run_log.record_dq_results(
                spark,
                run_id,
                run_date,
                table.name,
                rules,
                failure_counts,
                rows_checked=n_clean + n_quarantined,
            )

        written[table.name] = n_clean
        flag = f"  ({n_quarantined} quarantined)" if n_quarantined else ""
        print(f"silver.{table.name:<30} {n_clean:>8} rows{flag}")

    return written
