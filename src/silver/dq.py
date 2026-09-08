"""Data-quality rule registry, one entry per expectation.

Rules live here as data rather than as scattered `if` statements so the same
list drives three things: the split into clean/quarantine, the per-run results
written to ops.dq_results, and the documentation table in the README.

Severity:
  reject -- the row is diverted to silver.quarantine_<table>
  warn   -- the row loads, but the failure is still counted
"""

from __future__ import annotations

from pyspark.sql import functions as F

from src.silver.transforms import DQRule

VALID_ORDER_STATUSES = [
    "delivered", "shipped", "canceled", "unavailable",
    "invoiced", "processing", "created", "approved",
]

VALID_PAYMENT_TYPES = ["credit_card", "boleto", "voucher", "debit_card", "not_defined"]


def rules_for(table: str) -> list[DQRule]:
    """Return the rules for a silver table. Unknown tables get none."""
    return _RULES.get(table, [])


def _r(table: str, name: str, description: str, predicate, severity: str = "reject"):
    return DQRule(f"{table}.{name}", table, description, predicate, severity)


_RULES: dict[str, list[DQRule]] = {
    "orders": [
        _r("orders", "order_id_not_null", "order_id must be present",
           F.col("order_id").isNotNull()),
        _r("orders", "customer_id_not_null", "every order must have a customer",
           F.col("customer_id").isNotNull()),
        _r("orders", "status_in_domain", "order_status must be a known Olist status",
           F.col("order_status").isin(VALID_ORDER_STATUSES)),
        _r("orders", "purchase_ts_not_null", "purchase timestamp drives dim_date",
           F.col("order_purchase_timestamp").isNotNull()),
        # Warn, not reject: Olist genuinely contains deliveries recorded before
        # approval. Real data, worth surfacing, not worth dropping the order.
        _r("orders", "delivery_after_purchase",
           "delivered_customer_date should not precede purchase",
           F.col("order_delivered_customer_date").isNull()
           | (F.col("order_delivered_customer_date") >= F.col("order_purchase_timestamp")),
           severity="warn"),
    ],
    "order_items": [
        _r("order_items", "order_id_not_null", "must join to an order",
           F.col("order_id").isNotNull()),
        _r("order_items", "product_id_not_null", "must join to a product",
           F.col("product_id").isNotNull()),
        _r("order_items", "price_non_negative", "price >= 0",
           F.col("price") >= 0),
        _r("order_items", "freight_non_negative", "freight_value >= 0",
           F.col("freight_value") >= 0),
        _r("order_items", "item_sequence_positive", "order_item_id starts at 1",
           F.col("order_item_id") >= 1),
    ],
    "order_payments": [
        _r("order_payments", "order_id_not_null", "must join to an order",
           F.col("order_id").isNotNull()),
        _r("order_payments", "payment_value_non_negative", "payment_value >= 0",
           F.col("payment_value") >= 0),
        _r("order_payments", "payment_type_in_domain", "known payment type",
           F.col("payment_type").isin(VALID_PAYMENT_TYPES)),
        _r("order_payments", "installments_non_negative", "installments >= 0",
           F.col("payment_installments") >= 0),
    ],
    "customers": [
        _r("customers", "customer_id_not_null", "customer_id must be present",
           F.col("customer_id").isNotNull()),
        # The key that actually identifies a person. See docs/decisions.md.
        _r("customers", "unique_id_not_null",
           "customer_unique_id is the real natural key and must be present",
           F.col("customer_unique_id").isNotNull()),
        _r("customers", "state_two_letters", "Brazilian state code is 2 chars",
           F.length(F.col("customer_state")) == 2, severity="warn"),
    ],
    "products": [
        _r("products", "product_id_not_null", "product_id must be present",
           F.col("product_id").isNotNull()),
        _r("products", "weight_non_negative", "product_weight_g >= 0",
           F.col("product_weight_g").isNull() | (F.col("product_weight_g") >= 0)),
        # ~600 Olist products have no category. Kept and bucketed as 'unknown'
        # downstream rather than dropped -- they still carry real revenue.
        _r("products", "category_present", "product_category_name populated",
           F.col("product_category_name").isNotNull(), severity="warn"),
    ],
    "sellers": [
        _r("sellers", "seller_id_not_null", "seller_id must be present",
           F.col("seller_id").isNotNull()),
    ],
    "order_reviews": [
        _r("order_reviews", "review_id_not_null", "review_id must be present",
           F.col("review_id").isNotNull()),
        _r("order_reviews", "score_in_range", "review_score between 1 and 5",
           F.col("review_score").between(1, 5)),
    ],
}


def all_rules() -> list[DQRule]:
    return [rule for rules in _RULES.values() for rule in rules]
