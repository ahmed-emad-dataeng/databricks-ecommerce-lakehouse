# Databricks notebook source
# MAGIC %md
# MAGIC # M0 - one-time catalog setup
# MAGIC Creates the catalog, the five schemas and the landing volume.
# MAGIC Run this once by hand; it is deliberately NOT part of the job.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
for schema in ("landing", "bronze", "silver", "gold", "ops"):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.landing.raw")

print(f"Upload the 9 Olist CSVs to: /Volumes/{CATALOG}/landing/raw/olist/")
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG}"))
