# Databricks notebook source
# MAGIC %md
# MAGIC # Generate CDC change files and order events
# MAGIC Deterministic: same seed, same output, so a rerun is byte-identical.

# COMMAND ----------

# MAGIC %pip install faker==33.3.1
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from src.generator.make_deltas import build_change_day, read_head, write_csv, CHANGE_HEADER
import random

OLIST = f"/Volumes/{CATALOG}/landing/raw/olist"
CDC_OUT = f"/Volumes/{CATALOG}/landing/raw/cdc"

customers = read_head(f"{OLIST}/olist_customers_dataset.csv", 3 * 45)
for day in (1, 2, 3):
    rows = build_change_day(day, customers, random.Random(42 + day), 40, 5)
    write_csv(f"{CDC_OUT}/customers_changes_day{day}.csv", CHANGE_HEADER, rows)
    print(f"day {day}: {len(rows)} change rows")
