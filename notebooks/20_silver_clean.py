# Databricks notebook source
# MAGIC %md
# MAGIC # Silver - clean, dedupe, validate
# MAGIC Rows failing a `reject` rule land in `silver.quarantine_*`.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from src.silver.clean import clean_all
from src.ops.run_log import logged_task

with logged_task(spark, "silver_clean", RUN_ID, RUN_DATE) as m:
    counts = clean_all(spark, RUN_ID, RUN_DATE)
    m.rows_written = sum(counts.values())
    m.details = {k: str(v) for k, v in counts.items()}

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, rule_id, severity, rows_checked, rows_failed, failure_rate
    FROM {CATALOG}.ops.dq_results
    WHERE run_id = '{RUN_ID}' AND rows_failed > 0
    ORDER BY rows_failed DESC
"""))
