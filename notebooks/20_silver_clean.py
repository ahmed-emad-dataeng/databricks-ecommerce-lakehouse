# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - clean, dedupe, validate
# MAGIC Rows failing a `reject` rule land in `silver.quarantine_*`.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from src.silver.clean import clean_all
from src.ops.run_log import logged_task

from src.config import SCHEMA_BRONZE, SCHEMA_SILVER, SOURCE_TABLES, fqn

with logged_task(spark, "silver_clean", RUN_ID, RUN_DATE) as m:
    # Read from bronze BEFORE cleaning, so rows_read reflects the input rather
    # than being back-filled from the output. rows_read - (written +
    # quarantined) is then a real dedup figure instead of an unexplained gap.
    m.rows_read = sum(
        spark.table(fqn(SCHEMA_BRONZE, t.name)).count() for t in SOURCE_TABLES
    )

    counts = clean_all(spark, RUN_ID, RUN_DATE)
    m.rows_written = sum(counts.values())
    m.rows_quarantined = sum(
        spark.table(fqn(SCHEMA_SILVER, f"quarantine_{t.name}")).count()
        for t in SOURCE_TABLES
        if spark.catalog.tableExists(fqn(SCHEMA_SILVER, f"quarantine_{t.name}"))
    )
    m.details = {k: str(v) for k, v in counts.items()}
    print(
        f"read {m.rows_read:,} -> wrote {m.rows_written:,} "
        f"+ quarantined {m.rows_quarantined:,} "
        f"(deduped {m.rows_read - m.rows_written - m.rows_quarantined:,})"
    )

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, rule_id, severity, rows_checked, rows_failed, failure_rate
    FROM {CATALOG}.ops.dq_results
    WHERE run_id = '{RUN_ID}' AND rows_failed > 0
    ORDER BY rows_failed DESC
"""))
