"""Gold facts.

Three grains, three tables. The grain is stated in each table's COMMENT and is
the first thing to check when a number looks wrong:

  fact_order       one row per order
  fact_order_item  one row per product line within an order
  fact_payment     one row per payment record for an order

They are separate tables on purpose. Olist orders carry N payment rows
(installments, voucher + card splits), so folding payments into fact_order would
multiply order revenue by the payment count. See docs/decisions.md, trap #2.

Every fact is built with CREATE OR REPLACE from silver, which is itself a
deterministic function of bronze. Re-running rebuilds identical output rather
than appending, which is what makes `idempotency_check` pass.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from src.config import SCHEMA_GOLD, SCHEMA_SILVER, fqn

# --- The point-in-time join ----------------------------------------------
# Resolves each order to the customer VERSION that was current when the order
# was placed, rather than to the customer's present-day attributes. This is the
# reason dim_customer is SCD2 at all: joining on the natural key instead would
# silently restate history every time a customer moved.
POINT_IN_TIME_CUSTOMER = """
    LEFT JOIN {silver_customers} sc ON o.customer_id = sc.customer_id
    LEFT JOIN {dim_customer} dc
           ON sc.customer_unique_id = dc.customer_unique_id
          AND o.order_purchase_timestamp >= dc.effective_from
          AND o.order_purchase_timestamp <  dc.effective_to
"""


def _pit_clause() -> str:
    return POINT_IN_TIME_CUSTOMER.format(
        silver_customers=fqn(SCHEMA_SILVER, "customers"),
        dim_customer=fqn(SCHEMA_GOLD, "dim_customer"),
    )


def build_fact_order(spark: SparkSession) -> int:
    """One row per order, with delivery SLA measures derived here rather than in BI."""
    spark.sql(f"""
        CREATE OR REPLACE TABLE {fqn(SCHEMA_GOLD, 'fact_order')}
        COMMENT 'Order fact. GRAIN: one row per order (order_id).
                 customer_sk resolves to the customer version current at purchase time.'
        AS
        WITH items AS (
          SELECT order_id,
                 sum(price)              AS items_total,
                 sum(freight_value)      AS freight_total,
                 count(*)                AS items_count,
                 count(DISTINCT seller_id) AS distinct_sellers
          FROM {fqn(SCHEMA_SILVER, 'order_items')}
          GROUP BY order_id
        )
        SELECT
          o.order_id,
          dc.customer_sk,
          sc.customer_unique_id,
          ds.order_status_sk,
          cast(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_date_sk,
          cast(date_format(o.order_delivered_customer_date, 'yyyyMMdd') AS INT)
                                                                          AS delivered_date_sk,
          o.order_status,
          o.order_purchase_timestamp,
          o.order_approved_at,
          o.order_delivered_customer_date,
          o.order_estimated_delivery_date,
          coalesce(i.items_total, 0)                       AS items_total,
          coalesce(i.freight_total, 0)                     AS freight_total,
          coalesce(i.items_total, 0) + coalesce(i.freight_total, 0) AS order_total,
          coalesce(i.items_count, 0)                       AS items_count,
          coalesce(i.distinct_sellers, 0)                  AS distinct_sellers,
          -- Approval latency in hours; NULL when never approved.
          round((unix_timestamp(o.order_approved_at)
                 - unix_timestamp(o.order_purchase_timestamp)) / 3600.0, 2)
                                                           AS approval_lag_hours,
          datediff(o.order_delivered_customer_date, o.order_purchase_timestamp)
                                                           AS delivery_days,
          -- Positive = late. NULL while undelivered, so AVG ignores open orders
          -- instead of treating them as on-time.
          datediff(o.order_delivered_customer_date, o.order_estimated_delivery_date)
                                                           AS delivery_delay_days,
          o.order_delivered_customer_date > o.order_estimated_delivery_date
                                                           AS is_late,
          o.order_status IN ('canceled', 'unavailable')     AS is_cancelled,
          o.order_status = 'delivered'                      AS is_delivered
        FROM {fqn(SCHEMA_SILVER, 'orders')} o
        LEFT JOIN items i ON o.order_id = i.order_id
        LEFT JOIN {fqn(SCHEMA_GOLD, 'dim_order_status')} ds
               ON o.order_status = ds.order_status
        {_pit_clause()}
    """)
    return spark.table(fqn(SCHEMA_GOLD, "fact_order")).count()


def build_fact_order_item(spark: SparkSession) -> int:
    """One row per product line. The revenue fact everything else reconciles to."""
    spark.sql(f"""
        CREATE OR REPLACE TABLE {fqn(SCHEMA_GOLD, 'fact_order_item')}
        COMMENT 'Order-line fact. GRAIN: one row per product line within an order
                 (order_id, order_item_id). Olist has no quantity column -- each
                 row IS one unit, so quantity is fixed at 1 rather than invented.'
        AS
        SELECT
          i.order_id,
          i.order_item_id,
          dp.product_sk,
          dsel.seller_sk,
          dc.customer_sk,
          cast(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_date_sk,
          cast(date_format(i.shipping_limit_date, 'yyyyMMdd') AS INT) AS ship_limit_date_sk,
          dp.category,
          1                                    AS quantity,
          i.price                              AS item_price,
          i.freight_value,
          i.price + i.freight_value            AS item_revenue,
          o.order_status,
          o.order_status IN ('canceled', 'unavailable') AS is_cancelled
        FROM {fqn(SCHEMA_SILVER, 'order_items')} i
        JOIN {fqn(SCHEMA_SILVER, 'orders')} o ON i.order_id = o.order_id
        LEFT JOIN {fqn(SCHEMA_GOLD, 'dim_product')} dp ON i.product_id = dp.product_id
        LEFT JOIN {fqn(SCHEMA_GOLD, 'dim_seller')} dsel ON i.seller_id = dsel.seller_id
        {_pit_clause()}
    """)
    return spark.table(fqn(SCHEMA_GOLD, "fact_order_item")).count()


def build_fact_payment(spark: SparkSession) -> int:
    """One row per payment record. Wave 2.

    Kept at payment grain rather than aggregated onto fact_order, so that
    'revenue by payment method' is answerable without double-counting orders
    that were split across methods.
    """
    spark.sql(f"""
        CREATE OR REPLACE TABLE {fqn(SCHEMA_GOLD, 'fact_payment')}
        COMMENT 'Payment fact. GRAIN: one row per payment record for an order
                 (order_id, payment_sequential). An order may have several.'
        AS
        SELECT
          p.order_id,
          p.payment_sequential,
          dpt.payment_type_sk,
          dc.customer_sk,
          cast(date_format(o.order_purchase_timestamp, 'yyyyMMdd') AS INT) AS order_date_sk,
          p.payment_type,
          p.payment_installments,
          p.payment_value,
          o.order_status = 'delivered' AS order_completed
        FROM {fqn(SCHEMA_SILVER, 'order_payments')} p
        JOIN {fqn(SCHEMA_SILVER, 'orders')} o ON p.order_id = o.order_id
        LEFT JOIN {fqn(SCHEMA_GOLD, 'dim_payment_type')} dpt
               ON p.payment_type = dpt.payment_type
        {_pit_clause()}
    """)
    return spark.table(fqn(SCHEMA_GOLD, "fact_payment")).count()


# --- Informational constraints -------------------------------------------
# Unity Catalog does not enforce these, but declaring them makes Catalog
# Explorer render the star schema as an ERD and documents the joins for anyone
# reading the model cold.
def _constraints() -> list[tuple[str, str]]:
    """Built at call time: the REFERENCES clauses embed fully-qualified names,
    which depend on the run's catalog."""
    dim_product = fqn(SCHEMA_GOLD, "dim_product")
    dim_seller = fqn(SCHEMA_GOLD, "dim_seller")
    dim_date = fqn(SCHEMA_GOLD, "dim_date")
    dim_customer = fqn(SCHEMA_GOLD, "dim_customer")
    dim_order_status = fqn(SCHEMA_GOLD, "dim_order_status")
    dim_payment_type = fqn(SCHEMA_GOLD, "dim_payment_type")
    return [
        ("dim_date", "ALTER TABLE {t} ALTER COLUMN date_sk SET NOT NULL"),
        ("dim_date", "ALTER TABLE {t} ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_sk)"),
        ("dim_product", "ALTER TABLE {t} ALTER COLUMN product_sk SET NOT NULL"),
        (
            "dim_product",
            "ALTER TABLE {t} ADD CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)",
        ),
        ("dim_seller", "ALTER TABLE {t} ALTER COLUMN seller_sk SET NOT NULL"),
        (
            "dim_seller",
            "ALTER TABLE {t} ADD CONSTRAINT pk_dim_seller PRIMARY KEY (seller_sk)",
        ),
        ("dim_customer", "ALTER TABLE {t} ALTER COLUMN customer_sk SET NOT NULL"),
        (
            "dim_customer",
            "ALTER TABLE {t} ADD CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)",
        ),
        (
            "fact_order_item",
            "ALTER TABLE {t} ADD CONSTRAINT fk_foi_product "
            f"FOREIGN KEY (product_sk) REFERENCES {dim_product}",
        ),
        (
            "fact_order_item",
            "ALTER TABLE {t} ADD CONSTRAINT fk_foi_seller "
            f"FOREIGN KEY (seller_sk) REFERENCES {dim_seller}",
        ),
        (
            "fact_order_item",
            "ALTER TABLE {t} ADD CONSTRAINT fk_foi_date "
            f"FOREIGN KEY (order_date_sk) REFERENCES {dim_date}",
        ),
        (
            "fact_order",
            "ALTER TABLE {t} ADD CONSTRAINT fk_fo_date "
            f"FOREIGN KEY (order_date_sk) REFERENCES {dim_date}",
        ),
        # The two lookup dims and fact_payment were missing from the ERD, so
        # Catalog Explorer rendered the star with unconnected tables.
        ("dim_order_status", "ALTER TABLE {t} ALTER COLUMN order_status_sk SET NOT NULL"),
        (
            "dim_order_status",
            "ALTER TABLE {t} ADD CONSTRAINT pk_dim_order_status "
            "PRIMARY KEY (order_status_sk)",
        ),
        ("dim_payment_type", "ALTER TABLE {t} ALTER COLUMN payment_type_sk SET NOT NULL"),
        (
            "dim_payment_type",
            "ALTER TABLE {t} ADD CONSTRAINT pk_dim_payment_type "
            "PRIMARY KEY (payment_type_sk)",
        ),
        (
            "fact_order",
            "ALTER TABLE {t} ADD CONSTRAINT fk_fo_customer "
            f"FOREIGN KEY (customer_sk) REFERENCES {dim_customer}",
        ),
        (
            "fact_order",
            "ALTER TABLE {t} ADD CONSTRAINT fk_fo_status "
            f"FOREIGN KEY (order_status_sk) REFERENCES {dim_order_status}",
        ),
        (
            "fact_order_item",
            "ALTER TABLE {t} ADD CONSTRAINT fk_foi_customer "
            f"FOREIGN KEY (customer_sk) REFERENCES {dim_customer}",
        ),
        (
            "fact_payment",
            "ALTER TABLE {t} ADD CONSTRAINT fk_fp_customer "
            f"FOREIGN KEY (customer_sk) REFERENCES {dim_customer}",
        ),
        (
            "fact_payment",
            "ALTER TABLE {t} ADD CONSTRAINT fk_fp_type "
            f"FOREIGN KEY (payment_type_sk) REFERENCES {dim_payment_type}",
        ),
        (
            "fact_payment",
            "ALTER TABLE {t} ADD CONSTRAINT fk_fp_date "
            f"FOREIGN KEY (order_date_sk) REFERENCES {dim_date}",
        ),
    ]


def apply_constraints(spark: SparkSession) -> None:
    """Idempotent: a constraint that already exists is skipped, not fatal."""
    for table, template in _constraints():
        sql = template.format(t=fqn(SCHEMA_GOLD, table))
        try:
            spark.sql(sql)
        except Exception as exc:  # noqa: BLE001
            if "already exists" in str(exc).lower():
                continue
            print(f"  constraint skipped on {table}: {exc}")


def build_all(spark: SparkSession, include_wave2: bool = True) -> dict[str, int]:
    counts = {
        "fact_order": build_fact_order(spark),
        "fact_order_item": build_fact_order_item(spark),
    }
    if include_wave2:
        counts["fact_payment"] = build_fact_payment(spark)
    apply_constraints(spark)
    for name, n in counts.items():
        print(f"gold.{name:<20} {n:>8} rows")
    return counts
