# Databricks notebook source
# MAGIC %md
# MAGIC # CDC - apply one day of customer changes (SCD2)
# MAGIC `cdc_day=0` skips. Days 1-3 apply that change file.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from pyspark.sql import functions as F

from src.gold import dims
from src.ops.run_log import logged_task

if CDC_DAY == 0:
    print("cdc_day=0, nothing to apply")
else:
    path = f"/Volumes/{CATALOG}/landing/raw/cdc/customers_changes_day{CDC_DAY}.csv"
    changes = (
        spark.read.option("header", "true")
        .csv(path)
        .withColumn("updated_at", F.to_timestamp("updated_at"))
    )
    with logged_task(spark, f"cdc_apply_day{CDC_DAY}", RUN_ID, RUN_DATE) as m:
        m.rows_read = changes.count()
        counts = dims.apply_customer_changes(spark, changes, RUN_DATE)
        m.rows_written = counts["new"] + counts["changed"] + counts["deleted"]
        m.details = {k: str(v) for k, v in counts.items()}
        print(counts)
