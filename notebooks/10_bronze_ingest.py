# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze - land raw Olist as Delta
# MAGIC Auto Loader, `Trigger.AvailableNow()`, rescued-data column on.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from src.bronze.ingest import ingest_all
from src.ops.run_log import logged_task

with logged_task(spark, "bronze_ingest", RUN_ID, RUN_DATE) as m:
    counts = ingest_all(spark, batch_id=RUN_ID)
    # rows_read == rows_written at bronze: COPY INTO reports rows it actually
    # consumed from source files, and bronze applies no filtering. On a re-run
    # both are 0, which is the file-level idempotency guarantee showing up in
    # the run log rather than only in a doc.
    m.rows_read = sum(counts.values())
    m.rows_written = m.rows_read
    m.details = {k: str(v) for k, v in counts.items()}
