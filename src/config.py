"""Central configuration: catalog layout and the Olist source-table registry.

Everything downstream (bronze ingest, silver clean, DQ) loops over SOURCE_TABLES
rather than hardcoding nine near-identical code paths.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field

# --- Catalog layout -------------------------------------------------------
# The catalog is resolved at CALL time from an environment variable, never
# frozen at import time. The bundle passes a different catalog per target
# (ecommerce_dev for dev, ecommerce for prod) and notebooks/_bootstrap.py
# exports it before any src module runs.
#
# Freezing it in a module-level constant was a real bug: the job would receive
# `ecommerce_dev` as a parameter, hand it to modules that ignored it, and write
# everything to `ecommerce` instead -- a silent wrong-target failure rather than
# an error.
CATALOG_ENV_VAR = "ECOMMERCE_CATALOG"
DEFAULT_CATALOG = "ecommerce"

SCHEMA_LANDING = "landing"
SCHEMA_BRONZE = "bronze"
SCHEMA_SILVER = "silver"
SCHEMA_GOLD = "gold"
SCHEMA_OPS = "ops"

VOLUME = "raw"


def catalog() -> str:
    """Target catalog for this run. Env var wins; falls back to the default."""
    return os.environ.get(CATALOG_ENV_VAR) or DEFAULT_CATALOG


def set_catalog(name: str) -> None:
    """Pin the catalog for this process. Called from the notebook bootstrap."""
    os.environ[CATALOG_ENV_VAR] = name


def volume_root() -> str:
    return f"/Volumes/{catalog()}/{SCHEMA_LANDING}/{VOLUME}"


def path_olist() -> str:
    return f"{volume_root()}/olist"


def path_cdc() -> str:
    return f"{volume_root()}/cdc"


def path_events() -> str:
    return f"{volume_root()}/events"


def path_checkpoints() -> str:
    return f"{volume_root()}/_checkpoints"


def path_schemas() -> str:
    return f"{volume_root()}/_schemas"


# Metadata columns stamped onto every bronze table.
INGEST_TS = "_ingest_ts"
SOURCE_FILE = "_source_file"
BATCH_ID = "_batch_id"


def fqn(schema: str, table: str) -> str:
    """Fully-qualified Unity Catalog name, resolved against the live catalog."""
    return f"{catalog()}.{schema}.{table}"


@dataclass(frozen=True)
class SourceTable:
    """One Olist CSV and how to land, key and de-duplicate it.

    business_key: the grain of the *source* file, used for de-duplication.
    sequence_col: column that decides which duplicate wins. Olist has no
        `updated_at`, so most tables fall back to `_ingest_ts` and dedup is
        effectively "drop exact re-ingests". The CDC change files generated in
        M5 *do* carry a real `updated_at`, which is where ordering matters.
    rename: fixes Olist's shipped column-name typos (`lenght`).
    """

    name: str
    source_file: str
    business_key: tuple[str, ...]
    sequence_col: str | None = None
    timestamp_cols: tuple[str, ...] = ()
    string_cols: tuple[str, ...] = ()
    rename: dict[str, str] = field(default_factory=dict)

    @property
    def source_glob(self) -> str:
        # Property, not a stored field, so it follows the live catalog.
        return f"{path_olist()}/{self.source_file}"


CORE_TABLES: tuple[SourceTable, ...] = (
    SourceTable(
        name="customers",
        source_file="olist_customers_dataset.csv",
        # NOTE: the grain here is one row per customer_id, NOT per person.
        # customer_unique_id repeats across orders. See docs/decisions.md.
        business_key=("customer_id",),
        string_cols=("customer_city", "customer_state"),
    ),
    SourceTable(
        name="orders",
        source_file="olist_orders_dataset.csv",
        business_key=("order_id",),
        sequence_col="order_purchase_timestamp",
        timestamp_cols=(
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
        string_cols=("order_status",),
    ),
    SourceTable(
        name="order_items",
        source_file="olist_order_items_dataset.csv",
        business_key=("order_id", "order_item_id"),
        timestamp_cols=("shipping_limit_date",),
    ),
    SourceTable(
        name="order_payments",
        source_file="olist_order_payments_dataset.csv",
        business_key=("order_id", "payment_sequential"),
        string_cols=("payment_type",),
    ),
    SourceTable(
        name="order_reviews",
        source_file="olist_order_reviews_dataset.csv",
        # review_id is NOT unique in the raw file (~800 dupes across order_ids),
        # so the real grain is (review_id, order_id). A DQ rule flags this.
        business_key=("review_id", "order_id"),
        sequence_col="review_answer_timestamp",
        timestamp_cols=("review_creation_date", "review_answer_timestamp"),
        string_cols=("review_comment_title",),
    ),
    SourceTable(
        name="products",
        source_file="olist_products_dataset.csv",
        business_key=("product_id",),
        string_cols=("product_category_name",),
        rename={
            "product_name_lenght": "product_name_length",
            "product_description_lenght": "product_description_length",
        },
    ),
    SourceTable(
        name="sellers",
        source_file="olist_sellers_dataset.csv",
        business_key=("seller_id",),
        string_cols=("seller_city", "seller_state"),
    ),
    SourceTable(
        name="product_category_translation",
        source_file="product_category_name_translation.csv",
        business_key=("product_category_name",),
        string_cols=("product_category_name", "product_category_name_english"),
    ),
)

# geolocation is ~1M rows -- roughly two thirds of the entire Olist dataset --
# and NOTHING in the gold model or the seven business questions joins to it.
# dim_customer carries customer_zip_code_prefix directly; no lat/lng is used.
#
# It is therefore excluded by default: on a quota-limited Free Edition account,
# and especially on one without the LinkedIn limit increase, there is no reason
# to spend two thirds of the ingest budget on a table nothing reads. Including
# it takes the project from ~550K rows to ~1.55M.
#
# Enable it only if you add a geographic question to the dashboard:
#     SOURCE_TABLES = CORE_TABLES + OPTIONAL_TABLES
OPTIONAL_TABLES: tuple[SourceTable, ...] = (
    SourceTable(
        name="geolocation",
        source_file="olist_geolocation_dataset.csv",
        # Many readings per zip prefix; silver aggregates to one row per prefix.
        business_key=("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"),
        string_cols=("geolocation_city", "geolocation_state"),
    ),
)

SOURCE_TABLES: tuple[SourceTable, ...] = CORE_TABLES

SOURCE_TABLES_BY_NAME = {t.name: t for t in CORE_TABLES + OPTIONAL_TABLES}

# --- SCD2 -----------------------------------------------------------------
SCD2_END_OF_TIME = "9999-12-31 00:00:00"

# Attributes tracked for history on dim_customer. A change in any of these
# closes the current version and opens a new one.
DIM_CUSTOMER_TRACKED = ("customer_city", "customer_state", "customer_segment")
