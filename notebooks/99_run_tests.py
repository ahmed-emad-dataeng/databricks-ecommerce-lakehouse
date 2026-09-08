# Databricks notebook source
# MAGIC %md
# MAGIC # Run the test suite in-workspace
# MAGIC Local PySpark on Windows needs a JDK and winutils; running pytest here
# MAGIC uses the runtime's Spark instead. Screenshot the output for the README.

# COMMAND ----------

# MAGIC %pip install pytest==8.3.4
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import os

import pytest

# chdir to the bundle root so `pythonpath = ["."]` in pyproject.toml resolves.
os.chdir(os.path.abspath(os.path.join(os.getcwd(), "..")))
exit_code = pytest.main(["-v", "--no-header", "-p", "no:cacheprovider", "tests"])

assert exit_code == 0, f"pytest failed with exit code {exit_code}"
