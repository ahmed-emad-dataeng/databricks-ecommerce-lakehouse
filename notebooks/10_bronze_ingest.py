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
    m.rows_written = sum(counts.values())
    m.details = {k: str(v) for k, v in counts.items()}
