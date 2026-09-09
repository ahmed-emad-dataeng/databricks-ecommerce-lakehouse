"""Pure DataFrame -> DataFrame transforms.

Every function here takes a DataFrame and returns a DataFrame with no I/O, no
table reads and no writes. That is the whole point: it makes the logic that
actually carries correctness risk unit-testable (see tests/).

Kept deliberately plain. This is not a framework.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DQ_FAILED_RULES = "_dq_failed_rules"


# --- Cleaning -------------------------------------------------------------
def rename_columns(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    """Rename columns present in `mapping`; ignore ones the frame doesn't have."""
    for old, new in mapping.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
    return df


def normalise_strings(df: DataFrame, cols: tuple[str, ...] | list[str]) -> DataFrame:
    """Trim, collapse internal whitespace, lowercase, and map blanks to NULL.

    Olist ships city names with inconsistent casing and padding
    ("sao paulo", "Sao Paulo ", "SAO  PAULO"), which silently fragments any
    group-by. Blank-to-NULL matters because '' and NULL are different keys but
    mean the same thing here.
    """
    for c in cols:
        if c not in df.columns:
            continue
        cleaned = F.lower(F.trim(F.regexp_replace(F.col(c), r"\s+", " ")))
        df = df.withColumn(c, F.when(cleaned == "", None).otherwise(cleaned))
    return df


TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss"


def parse_timestamps(df: DataFrame, cols: tuple[str, ...] | list[str]) -> DataFrame:
    """Parse Olist's 'yyyy-MM-dd HH:mm:ss' strings into real timestamps.

    Uses `try_to_timestamp`, NOT `to_timestamp`. Serverless runs with ANSI mode
    enabled, where `to_timestamp` RAISES on unparseable input:

        [CANNOT_PARSE_TIMESTAMP] Text 'not-a-date' could not be parsed

    which would abort silver_clean on a single malformed date rather than
    nulling the value and letting a DQ rule quarantine the row -- the exact
    behaviour this layer exists to provide. An earlier version of this docstring
    claimed to_timestamp already did that; it does not, and
    test_parse_timestamps_nulls_unparseable_instead_of_raising is what proved it.
    """
    for c in cols:
        if c in df.columns:
            df = df.withColumn(
                c, F.try_to_timestamp(F.col(c), F.lit(TIMESTAMP_FORMAT))
            )
    return df


def dedupe_by_key(
    df: DataFrame,
    keys: tuple[str, ...] | list[str],
    order_by: str | None = None,
    tiebreak: str | None = None,
) -> DataFrame:
    """Keep one row per business key: latest `order_by`, then latest `tiebreak`.

    NULLs in `order_by` sort last (`desc_nulls_last`), so a row with a real
    timestamp always beats one without.
    """
    ordering: list[Column] = []
    for col_name in (order_by, tiebreak):
        if col_name and col_name in df.columns:
            ordering.append(F.col(col_name).desc_nulls_last())
    if not ordering:
        # No ordering column available: any row will do, but be deterministic.
        ordering = [F.col(k).asc() for k in keys]

    w = Window.partitionBy(*[F.col(k) for k in keys]).orderBy(*ordering)
    return (
        df.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


# --- Data quality ---------------------------------------------------------
@dataclass(frozen=True)
class DQRule:
    """A single expectation.

    predicate: rows for which this evaluates to TRUE are valid. A NULL result
        counts as a failure — otherwise `col > 0` would silently pass every row
        where `col` is NULL, which is exactly the bug this layer exists to catch.
    severity: "reject" routes the row to quarantine; "warn" only records it.
    """

    rule_id: str
    table: str
    description: str
    predicate: Column
    severity: str = "reject"


def tag_rule_failures(df: DataFrame, rules: list[DQRule]) -> DataFrame:
    """Add `_dq_failed_rules`: an array of the rule_ids this row violates."""
    if not rules:
        return df.withColumn(DQ_FAILED_RULES, F.array().cast("array<string>"))

    flags = [
        F.when(~F.coalesce(r.predicate, F.lit(False)), F.lit(r.rule_id))
        for r in rules
    ]
    return df.withColumn(DQ_FAILED_RULES, F.array_compact(F.array(*flags)))


def count_rule_failures(df: DataFrame, rules: list[DQRule]) -> DataFrame:
    """Per-rule failure counts over ALL rows, returning (rule_id, rows_failed).

    Deliberately not derived from the quarantine table: quarantine only holds
    rows that failed a `reject` rule, so counting from it would silently report
    zero failures for every `warn` rule -- and for warn failures on rows that
    passed all reject rules. Warn rules exist precisely to be counted without
    blocking the load, so they have to be counted here instead.
    """
    if not rules:
        return df.sparkSession.createDataFrame([], "rule_id string, rows_failed long")

    return (
        tag_rule_failures(df, rules)
        .select(F.explode(DQ_FAILED_RULES).alias("rule_id"))
        .groupBy("rule_id")
        .agg(F.count(F.lit(1)).alias("rows_failed"))
    )


def split_by_rules(
    df: DataFrame, rules: list[DQRule]
) -> tuple[DataFrame, DataFrame]:
    """Split into (clean, quarantined).

    Only "reject"-severity failures divert a row. "warn" failures stay in the
    clean set but are still recorded in `_dq_failed_rules`, so a warning is
    visible in ops.dq_results without blocking the load.
    """
    tagged = tag_rule_failures(df, rules)
    reject_ids = [r.rule_id for r in rules if r.severity == "reject"]

    if not reject_ids:
        return tagged.drop(DQ_FAILED_RULES), tagged.limit(0)

    rejected = F.size(
        F.array_intersect(
            F.col(DQ_FAILED_RULES), F.array(*[F.lit(i) for i in reject_ids])
        )
    ) > 0

    clean = tagged.filter(~rejected).drop(DQ_FAILED_RULES)
    quarantined = tagged.filter(rejected)
    return clean, quarantined


# --- Business logic -------------------------------------------------------
def aggregate_geolocation(df: DataFrame) -> DataFrame:
    """Collapse ~1M geolocation rows to one row per zip prefix.

    The raw table has many lat/lng readings per prefix. Averaging them gives a
    usable centroid and takes the table from ~1M rows to ~19k, which matters on
    a quota-limited account.
    """
    return df.groupBy("geolocation_zip_code_prefix").agg(
        F.avg("geolocation_lat").alias("latitude"),
        F.avg("geolocation_lng").alias("longitude"),
        # Most frequently reported city name for the prefix.
        F.mode("geolocation_city").alias("city"),
        F.mode("geolocation_state").alias("state"),
        F.count(F.lit(1)).alias("reading_count"),
    )


def compute_first_order_ts(orders: DataFrame) -> DataFrame:
    """Earliest order timestamp per customer. Deliberately NOT as-of filtered.

    A customer's first order date is immutable -- it is a property of the
    customer, not of the reporting window -- and it drives
    dim_customer.effective_from for the initial SCD2 version.

    Deriving it from an as-of-filtered aggregate is a real bug: customers whose
    first order falls after run_date get a NULL first_order_ts, hence a NULL
    effective_from, and `ts >= NULL` evaluates to NULL so every point-in-time
    join for them fails. Their facts get a NULL surrogate key with no error
    raised. Measured at run_date 2018-10-01 that was 1 orphaned order; at
    run_date 2017-01-01 it would be most of the dataset.

    Contrast with compute_customer_segment, which SHOULD be as-of filtered --
    a segment is a statement about behaviour up to a point in time.
    """
    return orders.groupBy("customer_unique_id").agg(
        F.min("order_purchase_timestamp").alias("first_order_ts")
    )


def compute_customer_segment(
    orders: DataFrame, as_of: Column, lapsed_days: int = 180, high_value: float = 1000.0
) -> DataFrame:
    """Derive an RFM-ish segment per customer_unique_id, as of a point in time.

    This is the *honest* driver for SCD Type 2 on dim_customer: the segment is
    recomputed every run and genuinely changes as a customer's behaviour ages,
    unlike a contrived city edit.

    Expects one row per order with: customer_unique_id, order_purchase_timestamp,
    order_total. Only orders on or before `as_of` are considered, so the result
    is reproducible for any historical run_date.
    """
    scoped = orders.filter(F.col("order_purchase_timestamp") <= as_of)

    agg = scoped.groupBy("customer_unique_id").agg(
        F.count(F.lit(1)).alias("order_count"),
        F.sum("order_total").alias("lifetime_value"),
        F.max("order_purchase_timestamp").alias("last_order_ts"),
        F.min("order_purchase_timestamp").alias("first_order_ts"),
    )

    days_since = F.datediff(as_of, F.col("last_order_ts"))

    return agg.withColumn(
        "customer_segment",
        F.when(F.col("lifetime_value") >= high_value, F.lit("high_value"))
        .when(days_since > lapsed_days, F.lit("lapsed"))
        .when(F.col("order_count") > 1, F.lit("returning"))
        .otherwise(F.lit("new")),
    ).withColumn("days_since_last_order", days_since)
