# Databricks notebook source
# MAGIC %md
# MAGIC # Idempotency check
# MAGIC Fingerprints gold and FAILS the job if a rerun of the same `run_date`
# MAGIC produced different output. The proof, not the promise.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from src.ops.idempotency import check

diffs = check(spark, RUN_ID, RUN_DATE, strict=True)
print("no differences" if not diffs else diffs)

# COMMAND ----------

display(spark.sql(f"""
    SELECT run_id, table_name, row_count, captured_at
    FROM {CATALOG}.ops.gold_fingerprints
    WHERE run_date = date('{RUN_DATE}')
    ORDER BY captured_at DESC, table_name
"""))
