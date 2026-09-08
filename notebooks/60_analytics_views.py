# Databricks notebook source
# MAGIC %md
# MAGIC # Analytical views
# MAGIC Rebuilds every view in `sql/views/`. The dashboard reads these, never the
# MAGIC fact tables directly, so a model change does not break the dashboard.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import glob
import os

from src.ops.run_log import logged_task

view_files = sorted(glob.glob(os.path.join(os.getcwd(), "..", "sql", "views", "*.sql")))

with logged_task(spark, "analytics_views", RUN_ID, RUN_DATE) as m:
    for path in view_files:
        with open(path, encoding="utf-8") as fh:
            body = fh.read()
        # Views are written against ${catalog}; substitute the target catalog.
        for statement in body.replace("${catalog}", CATALOG).split(";"):
            if statement.strip():
                spark.sql(statement)
        print(f"applied {os.path.basename(path)}")
    m.details = {"views_applied": str(len(view_files))}
