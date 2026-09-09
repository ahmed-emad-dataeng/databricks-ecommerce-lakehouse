"""Bronze: land the raw Olist CSVs as Delta, faithfully and incrementally.

Bronze keeps the source shape. No typing, no cleaning, no business rules -- only
the three provenance columns. Anything that reshapes data belongs in silver, so
that a mistake there can be fixed by replaying bronze instead of re-downloading.

Incremental strategy: Auto Loader (`cloudFiles`) in directory-listing mode with
`Trigger.AvailableNow()`. Free Edition is serverless-only, where time-based and
continuous triggers are rejected outright (INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED),
so AvailableNow is both the recommended and the only option.

`rescuedDataColumn` is on so a source column that stops matching the inferred
schema lands in `_rescued_data` instead of being silently dropped.

FALLBACK: if checkpointing into a UC volume misbehaves on serverless, switch
`INGEST_MODE` to "copy_into". COPY INTO tracks already-loaded files server-side
and needs no checkpoint, giving the same file-level idempotency. Test this in the
first 20 minutes of M2 rather than discovering it late.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.config import (
    BATCH_ID,
    INGEST_TS,
    SCHEMA_BRONZE,
    SOURCE_FILE,
    SOURCE_TABLES,
    SourceTable,
    fqn,
    path_checkpoints,
    path_olist,
    path_schemas,
)

INGEST_MODE = "auto_loader"  # or "copy_into"


def _format_options(table: SourceTable) -> str:
    """Render FORMAT_OPTIONS for COPY INTO, including per-table CSV options."""
    opts = {"header": "true", "inferSchema": "false", **table.csv_options}
    return ", ".join(f"'{k}' = '{v}'" for k, v in opts.items())


def _with_provenance(df: DataFrame, batch_id: str) -> DataFrame:
    """Stamp where each row came from and when it landed."""
    return (
        df.withColumn(INGEST_TS, F.current_timestamp())
        .withColumn(SOURCE_FILE, F.col("_metadata.file_path"))
        .withColumn(BATCH_ID, F.lit(batch_id))
    )


def ingest_auto_loader(spark: SparkSession, table: SourceTable, batch_id: str) -> int:
    """Stream one CSV into bronze, processing only files not yet seen."""
    target = fqn(SCHEMA_BRONZE, table.name)

    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", f"{path_schemas()}/{table.name}")
        .option("cloudFiles.inferColumnTypes", "false")  # bronze stays all-string
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("header", "true")
        .option("rescuedDataColumn", "_rescued_data")
        .options(**table.csv_options)  # e.g. multiLine for order_reviews
        .load(f"{path_olist()}/{table.source_file}")
    )

    query = (
        _with_provenance(stream, batch_id)
        .writeStream.option(
            "checkpointLocation", f"{path_checkpoints()}/bronze/{table.name}"
        )
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(target)
    )
    query.awaitTermination()

    progress = query.recentProgress
    return int(sum(p.get("numInputRows", 0) for p in progress))


def ingest_copy_into(spark: SparkSession, table: SourceTable, batch_id: str) -> int:
    """Fallback path: COPY INTO, which needs no checkpoint.

    Idempotent for the same reason Auto Loader is -- Databricks records which
    files a target table has already consumed and skips them on re-run.
    """
    target = fqn(SCHEMA_BRONZE, table.name)
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target}")

    before = spark.table(target).count() if spark.catalog.tableExists(target) else 0
    spark.sql(f"""
        COPY INTO {target}
        FROM (
          SELECT *,
                 current_timestamp() AS {INGEST_TS},
                 _metadata.file_path AS {SOURCE_FILE},
                 '{batch_id}'        AS {BATCH_ID}
          FROM '{path_olist()}/{table.source_file}'
        )
        FILEFORMAT = CSV
        FORMAT_OPTIONS ({_format_options(table)})
        COPY_OPTIONS ('mergeSchema' = 'true')
    """)
    return spark.table(target).count() - before


def ingest_table(spark: SparkSession, table: SourceTable, batch_id: str) -> int:
    if INGEST_MODE == "copy_into":
        return ingest_copy_into(spark, table, batch_id)
    return ingest_auto_loader(spark, table, batch_id)


def ingest_all(spark: SparkSession, batch_id: str) -> dict[str, int]:
    """Land every registered source table. One loop, not nine code paths."""
    counts: dict[str, int] = {}
    for table in SOURCE_TABLES:
        counts[table.name] = ingest_table(spark, table, batch_id)
        print(f"bronze.{table.name:<30} +{counts[table.name]:>8} rows")
    return counts
