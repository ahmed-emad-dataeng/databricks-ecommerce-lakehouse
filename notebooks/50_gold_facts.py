# Databricks notebook source
# MAGIC %md
# MAGIC # Gold facts
# MAGIC Three grains + informational PK/FK so Catalog Explorer renders the ERD.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from src.gold.facts import build_all
from src.ops.run_log import logged_task

with logged_task(spark, "gold_facts", RUN_ID, RUN_DATE) as m:
    counts = build_all(spark, include_wave2=True)
    m.rows_written = sum(counts.values())
    m.details = {k: str(v) for k, v in counts.items()}
