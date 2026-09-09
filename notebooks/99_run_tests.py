# Databricks notebook source
# MAGIC %md
# MAGIC # Run the test suite in-workspace
# MAGIC
# MAGIC Local PySpark on Windows needs a JDK and winutils, so the Spark-dependent
# MAGIC tests run here. `tests/test_config.py` and `tests/test_sql_files.py` need
# MAGIC no Spark and also run locally.
# MAGIC
# MAGIC Output is captured (**stdout and stderr**) and written to the landing
# MAGIC volume as well as printed. Notebook output is not retrievable through the
# MAGIC Jobs API, so without this a failing suite reports only an exit code.
# MAGIC pytest writes *usage* errors to stderr, so capturing stdout alone yields an
# MAGIC empty report for exactly the failures you most need to read.

# COMMAND ----------

# MAGIC %pip install pytest==8.3.4
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

import contextlib
import io
import os
import pathlib
import sys

# The workspace filesystem does not support creating __pycache__ directories,
# so importing anything under /Workspace raises
#   OSError: [Errno 95] Operation not supported: '.../tests/__pycache__'
# which pytest reports as a conftest ImportError and a bare exit code 4.
# `-p no:cacheprovider` disables pytest's OWN cache, not CPython's bytecode
# cache -- these two lines are what actually stop the write. Set before pytest
# imports any test module.
sys.dont_write_bytecode = True
os.environ["PYTHONDONTWRITEBYTECODE"] = "1"

import pytest

import src

# Derive the repo root from the package location rather than from cwd, which
# %restart_python can reset and which differs between a job task and a REPL.
REPO_ROOT = pathlib.Path(src.__file__).resolve().parent.parent
TESTS_DIR = REPO_ROOT / "tests"

diagnostics = [
    f"cwd          = {os.getcwd()}",
    f"src.__file__ = {src.__file__}",
    f"REPO_ROOT    = {REPO_ROOT}  exists={REPO_ROOT.is_dir()}",
    f"TESTS_DIR    = {TESTS_DIR}  exists={TESTS_DIR.is_dir()}",
    f"repo root contents = {sorted(p.name for p in REPO_ROOT.iterdir())[:20]}",
]
if TESTS_DIR.is_dir():
    diagnostics.append(
        f"test files   = {sorted(p.name for p in TESTS_DIR.glob('test_*.py'))}"
    )
print("\n".join(diagnostics))

# COMMAND ----------

os.chdir(REPO_ROOT)

buf = io.StringIO()
with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
    exit_code = pytest.main(
        [
            "-v",
            "--no-header",
            "-rA",
            "--tb=short",
            "-p",
            "no:cacheprovider",
            str(TESTS_DIR),
        ]
    )
report = buf.getvalue()

print(report)

REPORT_DIR = f"/Volumes/{CATALOG}/landing/raw/_reports"
os.makedirs(REPORT_DIR, exist_ok=True)
lines = ["exit_code=" + str(exit_code), ""] + diagnostics + ["", report]
with open(f"{REPORT_DIR}/pytest.txt", "w", encoding="utf-8") as fh:
    fh.write("\n".join(lines))
print("report written to " + REPORT_DIR + "/pytest.txt")

# COMMAND ----------

# pytest exit codes: 0 ok, 1 tests failed, 2 interrupted, 3 internal,
# 4 usage error, 5 no tests collected.
assert exit_code == 0, "pytest exit code " + str(exit_code)
