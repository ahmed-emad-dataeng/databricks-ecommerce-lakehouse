"""Gold dimensions.

Surrogate keys are deterministic hashes rather than monotonic sequences. That
is a deliberate trade: a hash costs a little readability but makes every build
reproducible, so re-running the job cannot renumber existing keys and silently
break already-written facts. Sequence-generated keys would need extra
bookkeeping to stay stable across replays.

  dim_date      -- generated, conformed
  dim_customer  -- SCD Type 2 on customer_unique_id (see scd2.py)
  dim_product   -- SCD Type 1 (deliberately: see docs/decisions.md)
  dim_seller    -- SCD Type 1
  dim_payment_type / dim_order_status -- small conformed dims (wave 2)
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from src.config import (
    DIM_CUSTOMER_TRACKED,
    SCD2_BEGINNING_OF_TIME,
    SCD2_END_OF_TIME,
    SCHEMA_GOLD,
    SCHEMA_OPS,
    SCHEMA_SILVER,
    fqn,
)
from src.gold import scd2
from src.silver.transforms import compute_customer_segment, compute_first_order_ts

def dim_customer_table() -> str:
    return fqn(SCHEMA_GOLD, "dim_customer")
UNKNOWN = "unknown"


# --- dim_date -------------------------------------------------------------
def build_dim_date(spark: SparkSession) -> int:
    """Conformed date dimension spanning the order history, plus a year of slack."""
    bounds = spark.sql(f"""
        SELECT date(min(order_purchase_timestamp)) AS lo,
               date(max(order_purchase_timestamp)) AS hi
        FROM {fqn(SCHEMA_SILVER, 'orders')}
    """).collect()[0]

    spark.sql(f"""
        CREATE OR REPLACE TABLE {fqn(SCHEMA_GOLD, 'dim_date')}
        COMMENT 'Conformed date dimension. Grain: one row per calendar day.'
        AS
        SELECT
          cast(date_format(d, 'yyyyMMdd') AS INT) AS date_sk,
          d                                       AS date,
          year(d)                                 AS year,
          quarter(d)                              AS quarter,
          month(d)                                AS month,
          date_format(d, 'MMMM')                  AS month_name,
          date_format(d, 'yyyy-MM')               AS year_month,
          day(d)                                  AS day_of_month,
          dayofweek(d)                            AS day_of_week,
          date_format(d, 'EEEE')                  AS day_name,
          weekofyear(d)                           AS week_of_year,
          dayofweek(d) IN (1, 7)                  AS is_weekend
        FROM (
          SELECT explode(sequence(
            date('{bounds['lo']}'),
            date_add(date('{bounds['hi']}'), 365),
            interval 1 day
          )) AS d
        )
    """)
    return spark.table(fqn(SCHEMA_GOLD, "dim_date")).count()


# --- dim_customer (SCD2) --------------------------------------------------
def order_totals(spark: SparkSession) -> DataFrame:
    """Order-level revenue, aggregated from item grain.

    Aggregating up from order_items -- rather than joining payments onto orders --
    is what avoids the payment fan-out described in docs/decisions.md.
    """
    return spark.sql(f"""
        SELECT o.order_id,
               o.customer_id,
               o.order_purchase_timestamp,
               coalesce(sum(i.price), 0)         AS items_total,
               coalesce(sum(i.freight_value), 0) AS freight_total,
               coalesce(sum(i.price), 0) + coalesce(sum(i.freight_value), 0)
                                                 AS order_total,
               count(i.order_item_id)            AS items_count
        FROM {fqn(SCHEMA_SILVER, 'orders')} o
        LEFT JOIN {fqn(SCHEMA_SILVER, 'order_items')} i USING (order_id)
        GROUP BY o.order_id, o.customer_id, o.order_purchase_timestamp
    """)


def customer_attributes(spark: SparkSession, as_of: str) -> DataFrame:
    """One row per PERSON (customer_unique_id) with attributes and segment.

    Olist's customers table is one row per customer_id, i.e. per order, so it
    must be collapsed to customer_unique_id first. Getting this wrong is the
    single most common bug in Olist projects -- see docs/decisions.md.
    """
    customers = spark.table(fqn(SCHEMA_SILVER, "customers"))
    totals = order_totals(spark)

    with_person = totals.join(
        customers.select("customer_id", "customer_unique_id"), "customer_id", "left"
    )
    segments = compute_customer_segment(with_person, F.lit(as_of).cast("timestamp"))

    # Most recent address on record for the person, not an arbitrary one.
    latest_address = (
        customers.join(
            totals.select("customer_id", "order_purchase_timestamp"),
            "customer_id",
            "left",
        )
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("customer_unique_id").orderBy(
                    F.col("order_purchase_timestamp").desc_nulls_last()
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .select(
            "customer_unique_id",
            "customer_city",
            "customer_state",
            "customer_zip_code_prefix",
        )
    )

    # first_order_ts comes from the UNFILTERED aggregate: it drives
    # effective_from and must not depend on run_date. Everything else here is
    # legitimately as-of dependent.
    first_order = compute_first_order_ts(with_person)

    return (
        latest_address.join(
            segments.select(
                "customer_unique_id",
                "customer_segment",
                "order_count",
                "lifetime_value",
                "last_order_ts",
            ),
            "customer_unique_id",
            "left",
        )
        .join(first_order, "customer_unique_id", "left")
        .fillna({"customer_segment": UNKNOWN, "order_count": 0})
    )


def create_dim_customer(spark: SparkSession) -> None:
    """DDL for the SCD2 dimension. Separate from the load so the schema is
    explicit and reviewable rather than inferred from whatever arrived first."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {dim_customer_table()} (
          customer_sk              BIGINT  COMMENT 'Surrogate key. One per VERSION, not per customer.',
          customer_unique_id       STRING  COMMENT 'Natural key: identifies the person across orders.',
          customer_city            STRING,
          customer_state           STRING,
          customer_zip_code_prefix INT,
          customer_segment         STRING  COMMENT 'RFM-ish; recomputed each run, drives SCD2 change',
          order_count              BIGINT,
          lifetime_value           DECIMAL(12,2),
          first_order_ts           TIMESTAMP,
          last_order_ts            TIMESTAMP,
          _row_hash                BIGINT  COMMENT 'Hash of tracked attributes; change detection',
          effective_from           TIMESTAMP,
          effective_to             TIMESTAMP,
          is_current               BOOLEAN,
          is_deleted               BOOLEAN COMMENT 'Tombstone from a CDC delete; history preserved',
          _change_reason           STRING
        )
        COMMENT 'SCD Type 2 customer dimension. Grain: one row per customer version.
                 Point-in-time joins use customer_sk; the live population is
                 is_current AND NOT is_deleted.'
    """)


def seed_dim_customer(spark: SparkSession, as_of: str) -> int:
    """Initial load: version 1 for every customer.

    Idempotent by guard rather than by MERGE -- if the table already holds rows,
    this is a no-op and the CDC path takes over.
    """
    create_dim_customer(spark)
    if spark.table(dim_customer_table()).count() > 0:
        print("dim_customer already seeded; skipping initial load")
        return 0

    attrs = customer_attributes(spark, as_of)
    hashed = scd2.add_row_hash(attrs, DIM_CUSTOMER_TRACKED)

    (
        # Floored: a NULL effective_from would orphan every fact row for this
        # customer in the point-in-time join, silently.
        hashed.withColumn(
            "effective_from",
            F.coalesce(
                F.col("first_order_ts"),
                F.lit(SCD2_BEGINNING_OF_TIME).cast("timestamp"),
            ),
        )
        .withColumn("effective_to", F.lit(SCD2_END_OF_TIME).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("is_deleted", F.lit(False))
        .withColumn("_change_reason", F.lit("initial_load"))
        .withColumn(
            "customer_sk",
            F.xxhash64(
                F.concat_ws(
                    "||",
                    F.col("customer_unique_id"),
                    F.col("effective_from").cast("string"),
                )
            ),
        )
        # Cast every column to the DDL's declared type, don't just name-select.
        # lifetime_value is a sum of sums, which Spark widens to ~decimal(31,2)
        # against a DECIMAL(12,2) column; append-time schema enforcement rejects
        # that. Casting to the target schema also keeps the two write paths
        # (this seed and the SCD2 merge) structurally identical.
        .transform(lambda df: scd2.conform_to_table(spark, df, dim_customer_table()))
        .write.mode("append")
        .saveAsTable(dim_customer_table())
    )
    return spark.table(dim_customer_table()).count()


def apply_customer_changes(spark: SparkSession, changes: DataFrame, as_of: str) -> dict:
    """Apply one day of CDC change rows to the SCD2 dimension."""
    from src.silver.transforms import dedupe_by_key

    # One row per person per batch, latest updated_at wins. The generator plants
    # a duplicate whose later timestamp sits in an arbitrary file position, so
    # anything that relies on file order fails here.
    deduped = dedupe_by_key(changes, ("customer_unique_id",), order_by="updated_at")

    current = spark.table(dim_customer_table()).filter(F.col("is_current"))
    classified = scd2.classify_changes(
        current, deduped, "customer_unique_id", DIM_CUSTOMER_TRACKED, op_col="op"
    )

    return scd2.apply_scd2(
        spark,
        dim_customer_table(),
        classified,
        natural_key="customer_unique_id",
        attribute_cols=DIM_CUSTOMER_TRACKED,
        effective_from=F.lit(as_of).cast("timestamp"),
        staging_table=fqn(SCHEMA_OPS, "_stg_customer_changes"),
    )


# --- SCD1 dimensions ------------------------------------------------------
def build_dim_product(spark: SparkSession) -> int:
    """SCD Type 1. Products with no category are bucketed, not dropped -- they
    still carry real revenue, and dropping them would understate totals."""
    spark.sql(f"""
        CREATE OR REPLACE TABLE {fqn(SCHEMA_GOLD, 'dim_product')}
        COMMENT 'SCD Type 1 product dimension. Grain: one row per product.'
        AS
        SELECT
          xxhash64(p.product_id)                              AS product_sk,
          p.product_id,
          coalesce(p.product_category_name, '{UNKNOWN}')      AS category_pt,
          coalesce(t.product_category_name_english, '{UNKNOWN}') AS category,
          p.product_weight_g,
          p.product_length_cm,
          p.product_height_cm,
          p.product_width_cm,
          p.product_photos_qty
        FROM {fqn(SCHEMA_SILVER, 'products')} p
        LEFT JOIN {fqn(SCHEMA_SILVER, 'product_category_translation')} t
               ON p.product_category_name = t.product_category_name
    """)
    return spark.table(fqn(SCHEMA_GOLD, "dim_product")).count()


def build_dim_seller(spark: SparkSession) -> int:
    spark.sql(f"""
        CREATE OR REPLACE TABLE {fqn(SCHEMA_GOLD, 'dim_seller')}
        COMMENT 'SCD Type 1 seller dimension. Grain: one row per seller.'
        AS
        SELECT xxhash64(seller_id) AS seller_sk,
               seller_id,
               seller_city,
               seller_state,
               seller_zip_code_prefix
        FROM {fqn(SCHEMA_SILVER, 'sellers')}
    """)
    return spark.table(fqn(SCHEMA_GOLD, "dim_seller")).count()


def build_small_dims(spark: SparkSession) -> dict[str, int]:
    """Wave 2: conformed lookup dims for status and payment type."""
    spark.sql(f"""
        CREATE OR REPLACE TABLE {fqn(SCHEMA_GOLD, 'dim_order_status')}
        COMMENT 'Conformed order-status dimension. Grain: one row per status.'
        AS
        SELECT xxhash64(order_status) AS order_status_sk,
               order_status,
               order_status IN ('delivered')            AS is_completed,
               order_status IN ('canceled', 'unavailable') AS is_cancelled
        FROM (SELECT DISTINCT order_status FROM {fqn(SCHEMA_SILVER, 'orders')})
    """)
    spark.sql(f"""
        CREATE OR REPLACE TABLE {fqn(SCHEMA_GOLD, 'dim_payment_type')}
        COMMENT 'Conformed payment-type dimension. Grain: one row per payment type.'
        AS
        SELECT xxhash64(payment_type) AS payment_type_sk,
               payment_type,
               payment_type = 'credit_card' AS supports_installments
        FROM (SELECT DISTINCT payment_type FROM {fqn(SCHEMA_SILVER, 'order_payments')})
    """)
    return {
        "dim_order_status": spark.table(fqn(SCHEMA_GOLD, "dim_order_status")).count(),
        "dim_payment_type": spark.table(fqn(SCHEMA_GOLD, "dim_payment_type")).count(),
    }
