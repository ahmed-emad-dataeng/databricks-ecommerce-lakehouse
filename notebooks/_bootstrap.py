# Databricks notebook source
# MAGIC %md
# MAGIC Shared bootstrap: puts the bundle root on sys.path so `import src.*` works,
# MAGIC and reads the job parameters. Run via `%run ./_bootstrap` from each task.

# COMMAND ----------

import os
import sys

# Notebooks execute with cwd set to their own directory; `src/` is one level up.
_repo_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _repo_root not in sys.path:
    sys.path.insert(0, _repo_root)

dbutils.widgets.text("catalog", "ecommerce_dev")
dbutils.widgets.text("run_date", "2018-10-01")
dbutils.widgets.text("cdc_day", "0")

CATALOG = dbutils.widgets.get("catalog")
RUN_DATE = dbutils.widgets.get("run_date")
CDC_DAY = int(dbutils.widgets.get("cdc_day") or 0)

# Pin the catalog for every src module in this process. They resolve it at call
# time from this env var, so the job parameter actually takes effect -- without
# this, modules fall back to config.DEFAULT_CATALOG and would silently write to
# the wrong catalog.
from src.config import set_catalog  # noqa: E402  (must follow sys.path setup)

set_catalog(CATALOG)

# Job run id if orchestrated, a local uuid when run by hand.
try:
    RUN_ID = dbutils.notebook.entry_point.getDbutils().notebook().getContext().jobRunId().get()
except Exception:
    import uuid

    RUN_ID = f"manual-{uuid.uuid4().hex[:8]}"

print(f"catalog={CATALOG} run_date={RUN_DATE} cdc_day={CDC_DAY} run_id={RUN_ID}")
