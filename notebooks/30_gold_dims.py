# Databricks notebook source
# MAGIC %md
# MAGIC # Gold dimensions
# MAGIC dim_date, dim_customer (SCD2 seed), dim_product, dim_seller, small dims.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from src.gold import dims
from src.ops.run_log import logged_task

with logged_task(spark, "gold_dims", RUN_ID, RUN_DATE) as m:
    built = {
        "dim_date": dims.build_dim_date(spark),
        "dim_customer": dims.seed_dim_customer(spark, RUN_DATE),
        "dim_product": dims.build_dim_product(spark),
        "dim_seller": dims.build_dim_seller(spark),
    }
    built.update(dims.build_small_dims(spark))
    m.rows_written = sum(built.values())
    m.details = {k: str(v) for k, v in built.items()}
    print(built)
