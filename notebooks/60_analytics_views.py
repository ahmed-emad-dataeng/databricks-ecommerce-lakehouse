# Databricks notebook source
# MAGIC %md
# MAGIC # Analytical views
# MAGIC Rebuilds every view in `sql/views/`. The dashboard reads these, never the
# MAGIC fact tables directly, so a model change does not break the dashboard.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from src.ops.run_log import logged_task
from src.ops.sql_files import load_view_statements

# Splitting is in src/ops/sql_files.py, not inline here, because naive
# body.split(";") breaks on the prose semicolons in two of the view files --
# see tests/test_sql_files.py.
statements = load_view_statements(CATALOG)

with logged_task(spark, "analytics_views", RUN_ID, RUN_DATE) as m:
    applied = []
    for name, sql in statements:
        spark.sql(sql)
        applied.append(name)
    files = sorted(set(applied))
    print(f"applied {len(applied)} statements from {len(files)} files")
    for f in files:
        print(f"  {f}")
    m.details = {"statements": str(len(applied)), "files": str(len(files))}

# COMMAND ----------

# MAGIC %md Smoke-test the views the dashboard will read.

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {CATALOG}.gold.v_repeat_purchase_rate"))
display(spark.sql(f"SELECT * FROM {CATALOG}.gold.v_fanout_demo"))
display(spark.sql(f"SELECT * FROM {CATALOG}.gold.v_repeat_rate_key_comparison"))
