# Databricks notebook source
# MAGIC %md
# MAGIC # Data-quality probe — prove the quarantine path actually works
# MAGIC
# MAGIC Olist is clean enough that **no `reject` rule fires on the real data**, so
# MAGIC after a normal load every `silver.quarantine_*` table is empty. That means
# MAGIC the quarantine path is unproven: an empty quarantine table looks identical
# MAGIC whether the routing works or is silently broken.
# MAGIC
# MAGIC This injects a known-bad batch into `bronze.order_items`, runs the **real**
# MAGIC `clean_all()` code path, asserts every expected rule fired, then restores
# MAGIC bronze with Delta time travel and rebuilds silver.
# MAGIC
# MAGIC **Not part of the scheduled job** — it mutates bronze on purpose. Run it by
# MAGIC hand when you want the evidence.
# MAGIC
# MAGIC Leaves the lakehouse exactly as it found it; the final cell asserts that.

# COMMAND ----------

# MAGIC %run ./_bootstrap

# COMMAND ----------

from pyspark.sql import functions as F

from src.config import SCHEMA_BRONZE, SCHEMA_SILVER, fqn
from src.silver.clean import clean_all

TABLE = "order_items"
BRONZE = fqn(SCHEMA_BRONZE, TABLE)
SILVER = fqn(SCHEMA_SILVER, TABLE)
QUARANTINE = fqn(SCHEMA_SILVER, f"quarantine_{TABLE}")
PROBE_RUN_ID = f"dq_probe-{RUN_ID}"

# One row per reject rule on order_items, plus a NULL case, a two-rule case, and
# a clean control that must survive.
#
#  (order_id, order_item_id, product_id, seller_id, ship_ts, price, freight)
PROBE_ROWS = [
    (None, "1", "prod_x", "sell_x", "2018-01-01 00:00:00", "10.00", "5.00"),
    ("dqprobe_2", "1", None, "sell_x", "2018-01-01 00:00:00", "10.00", "5.00"),
    ("dqprobe_3", "1", "prod_x", "sell_x", "2018-01-01 00:00:00", "-10.00", "5.00"),
    ("dqprobe_4", "1", "prod_x", "sell_x", "2018-01-01 00:00:00", "10.00", "-5.00"),
    ("dqprobe_5", "0", "prod_x", "sell_x", "2018-01-01 00:00:00", "10.00", "5.00"),
    # THE IMPORTANT ONE: price IS NULL makes `price >= 0` evaluate to NULL, not
    # False. A naive filter lets this through. It must be quarantined.
    ("dqprobe_6", "1", "prod_x", "sell_x", "2018-01-01 00:00:00", None, "5.00"),
    # Two rules at once -- _dq_failed_rules must collect both, not just the first.
    ("dqprobe_7", "1", "prod_x", "sell_x", "2018-01-01 00:00:00", "-1.00", "-1.00"),
    # Clean control: must reach silver, proving the probe does not quarantine
    # indiscriminately.
    ("dqprobe_8", "1", "prod_x", "sell_x", "2018-01-01 00:00:00", "10.00", "5.00"),
]

EXPECTED = {
    "order_items.order_id_not_null": 1,
    "order_items.product_id_not_null": 1,
    "order_items.price_non_negative": 3,  # rows 3, 6 (NULL), 7
    "order_items.freight_non_negative": 2,  # rows 4, 7
    "order_items.item_sequence_positive": 1,
}
EXPECT_QUARANTINED = 7
EXPECT_CLEAN_DELTA = 1  # only the control row

# COMMAND ----------

# MAGIC %md ## 1. Baseline

# COMMAND ----------

base_version = spark.sql(f"DESCRIBE HISTORY {BRONZE} LIMIT 1").collect()[0]["version"]
base_bronze = spark.table(BRONZE).count()
base_silver = spark.table(SILVER).count()
base_quarantine = spark.table(QUARANTINE).count()

print(f"bronze version      : {base_version}")
print(f"bronze rows         : {base_bronze:,}")
print(f"silver rows         : {base_silver:,}")
print(f"quarantine rows     : {base_quarantine:,}   <- expected 0 on clean Olist")

# COMMAND ----------

# MAGIC %md ## 2. Inject the bad batch into bronze

# COMMAND ----------

probe_df = (
    spark.createDataFrame(
        PROBE_ROWS,
        "order_id string, order_item_id string, product_id string, seller_id string, "
        "shipping_limit_date string, price string, freight_value string",
    )
    .withColumn("_ingest_ts", F.current_timestamp())
    .withColumn("_source_file", F.lit("dq_probe/synthetic"))
    .withColumn("_batch_id", F.lit(PROBE_RUN_ID))
    .select(*[f.name for f in spark.table(BRONZE).schema.fields])
)
probe_df.write.mode("append").saveAsTable(BRONZE)
print(f"appended {len(PROBE_ROWS)} probe rows -> bronze rows now {spark.table(BRONZE).count():,}")

# COMMAND ----------

# MAGIC %md ## 3. Run the REAL silver path

# COMMAND ----------

clean_all(spark, PROBE_RUN_ID, RUN_DATE, tables=[TABLE])

# COMMAND ----------

# MAGIC %md ## 4. Assert the routing worked

# COMMAND ----------

failures = []

quarantined = spark.table(QUARANTINE)
n_quarantined = quarantined.count()
n_silver = spark.table(SILVER).count()

if n_quarantined != EXPECT_QUARANTINED:
    failures.append(f"quarantined {n_quarantined}, expected {EXPECT_QUARANTINED}")
if n_silver != base_silver + EXPECT_CLEAN_DELTA:
    failures.append(
        f"silver {n_silver}, expected {base_silver + EXPECT_CLEAN_DELTA} "
        "(clean control row must flow through)"
    )

# Per-rule counts, read out of the array column that split_by_rules writes.
actual = {
    r["rule_id"]: r["n"]
    for r in quarantined.select(F.explode("_dq_failed_rules").alias("rule_id"))
    .groupBy("rule_id")
    .agg(F.count(F.lit(1)).alias("n"))
    .collect()
}
for rule, expected_n in EXPECTED.items():
    if actual.get(rule) != expected_n:
        failures.append(f"{rule}: got {actual.get(rule)}, expected {expected_n}")

# The two-rule row must carry BOTH rule ids, not just the first match.
two_rule = (
    quarantined.filter(F.col("order_id") == "dqprobe_7")
    .select("_dq_failed_rules")
    .collect()
)
if not two_rule or len(two_rule[0]["_dq_failed_rules"]) != 2:
    failures.append(f"dqprobe_7 should list 2 failed rules, got {two_rule}")

# The clean control must be in silver and NOT in quarantine.
if spark.table(SILVER).filter(F.col("order_id") == "dqprobe_8").count() != 1:
    failures.append("clean control row dqprobe_8 missing from silver")
if quarantined.filter(F.col("order_id") == "dqprobe_8").count() != 0:
    failures.append("clean control row dqprobe_8 was wrongly quarantined")

print("--- per-rule failure counts ---")
for rule in sorted(set(EXPECTED) | set(actual)):
    mark = "OK" if actual.get(rule) == EXPECTED.get(rule) else "MISMATCH"
    print(f"  [{mark}] {rule}: {actual.get(rule)} (expected {EXPECTED.get(rule)})")

display(quarantined.select("order_id", "price", "freight_value", "_dq_failed_rules"))

# COMMAND ----------

# MAGIC %md ## 5. ops.dq_results must record it too

# COMMAND ----------

dq = spark.sql(f"""
    SELECT rule_id, severity, rows_checked, rows_failed, failure_rate
    FROM {CATALOG}.ops.dq_results
    WHERE run_id = '{PROBE_RUN_ID}' AND table_name = '{TABLE}'
    ORDER BY rows_failed DESC
""")
display(dq)

recorded = {r["rule_id"]: r["rows_failed"] for r in dq.collect()}
for rule, expected_n in EXPECTED.items():
    if recorded.get(rule) != expected_n:
        failures.append(
            f"ops.dq_results {rule}: got {recorded.get(rule)}, expected {expected_n}"
        )

# COMMAND ----------

# MAGIC %md ## 6. Restore — leave no trace
# MAGIC
# MAGIC Bronze goes back via Delta time travel. Silver is a deterministic rebuild
# MAGIC from bronze, so re-running the same path restores it rather than needing
# MAGIC its own restore — which is the pipeline's own idempotency property doing
# MAGIC the work.

# COMMAND ----------

spark.sql(f"RESTORE TABLE {BRONZE} TO VERSION AS OF {base_version}")
clean_all(spark, f"{PROBE_RUN_ID}-restore", RUN_DATE, tables=[TABLE])

after_bronze = spark.table(BRONZE).count()
after_silver = spark.table(SILVER).count()
after_quarantine = spark.table(QUARANTINE).count()

if after_bronze != base_bronze:
    failures.append(f"bronze not restored: {after_bronze} vs {base_bronze}")
if after_silver != base_silver:
    failures.append(f"silver not restored: {after_silver} vs {base_silver}")
if after_quarantine != base_quarantine:
    failures.append(f"quarantine not restored: {after_quarantine} vs {base_quarantine}")

print(f"bronze     {base_bronze:,} -> {after_bronze:,}")
print(f"silver     {base_silver:,} -> {after_silver:,}")
print(f"quarantine {base_quarantine:,} -> {after_quarantine:,}")

# COMMAND ----------

# MAGIC %md ## 7. Verdict

# COMMAND ----------

if failures:
    raise AssertionError("DQ PROBE FAILED:\n  " + "\n  ".join(failures))

print(
    "DQ PROBE PASSED\n"
    f"  {EXPECT_QUARANTINED} bad rows routed to quarantine, all 5 reject rules fired\n"
    "  NULL price quarantined (predicate NULL treated as failure)\n"
    "  two-rule row recorded both rule ids\n"
    "  clean control row flowed through to silver\n"
    "  ops.dq_results counts match\n"
    "  bronze/silver/quarantine all restored to baseline"
)
