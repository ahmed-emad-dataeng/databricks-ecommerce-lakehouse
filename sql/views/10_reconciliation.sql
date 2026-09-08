-- CORRECTNESS CHECKS. These are the queries to run before trusting a dashboard.
--
-- The idempotency task proves runs are repeatable; these prove the numbers are
-- right. Repeatable and wrong is still wrong.

-- TRAP #2, DEMONSTRATED: payment fan-out.
-- Olist orders carry N payment rows (installments, voucher + card splits), so
-- joining fact_payment onto fact_order multiplies order revenue by the payment
-- count. Keeping payments as their own fact at their own grain is the fix.
CREATE OR REPLACE VIEW ${catalog}.gold.v_fanout_demo
COMMENT 'Shows how joining payments to orders inflates revenue. Grain: one row.'
AS
SELECT
  (SELECT round(sum(order_total), 2) FROM ${catalog}.gold.fact_order)
                                                       AS revenue_correct,
  (SELECT round(sum(fo.order_total), 2)
     FROM ${catalog}.gold.fact_order fo
     JOIN ${catalog}.gold.fact_payment fp ON fo.order_id = fp.order_id)
                                                       AS revenue_inflated_by_fanout,
  (SELECT count(*) FROM ${catalog}.gold.fact_order)     AS orders,
  (SELECT count(*) FROM ${catalog}.gold.fact_payment)   AS payment_records,
  (SELECT round(count(*) * 1.0 / (SELECT count(DISTINCT order_id)
                                    FROM ${catalog}.gold.fact_payment), 3)
     FROM ${catalog}.gold.fact_payment)                 AS avg_payments_per_order;

-- Gold must reconcile to silver. A mismatch means a join dropped or duplicated
-- rows somewhere between the layers.
CREATE OR REPLACE VIEW ${catalog}.gold.v_reconciliation
COMMENT 'Row-count and revenue reconciliation, silver vs gold. Grain: one row per check.'
AS
SELECT 'order_items rows' AS check_name,
       (SELECT count(*) FROM ${catalog}.silver.order_items)      AS silver_value,
       (SELECT count(*) FROM ${catalog}.gold.fact_order_item)    AS gold_value
UNION ALL
SELECT 'orders rows',
       (SELECT count(*) FROM ${catalog}.silver.orders),
       (SELECT count(*) FROM ${catalog}.gold.fact_order)
UNION ALL
SELECT 'item revenue (rounded)',
       (SELECT round(sum(price + freight_value)) FROM ${catalog}.silver.order_items),
       (SELECT round(sum(item_revenue)) FROM ${catalog}.gold.fact_order_item)
UNION ALL
SELECT 'payment records',
       (SELECT count(*) FROM ${catalog}.silver.order_payments),
       (SELECT count(*) FROM ${catalog}.gold.fact_payment);

-- SCD2 structural integrity. Every row here is a bug: a natural key with zero
-- or several current versions, or overlapping validity windows.
CREATE OR REPLACE VIEW ${catalog}.gold.v_scd2_integrity
COMMENT 'SCD2 violations in dim_customer. Grain: one row per offending customer. Empty = healthy.'
AS
WITH per_key AS (
  SELECT
    customer_unique_id,
    count(*)                    AS versions,
    count_if(is_current)        AS current_versions,
    min(effective_from)         AS first_from,
    max(effective_to)           AS last_to
  FROM ${catalog}.gold.dim_customer
  GROUP BY customer_unique_id
),
overlaps AS (
  SELECT a.customer_unique_id, count(*) AS overlapping_pairs
  FROM ${catalog}.gold.dim_customer a
  JOIN ${catalog}.gold.dim_customer b
    ON a.customer_unique_id = b.customer_unique_id
   AND a.customer_sk <> b.customer_sk
   AND a.effective_from < b.effective_to
   AND b.effective_from < a.effective_to
  GROUP BY a.customer_unique_id
)
SELECT
  p.customer_unique_id,
  p.versions,
  p.current_versions,
  coalesce(o.overlapping_pairs, 0) AS overlapping_pairs,
  CASE
    WHEN p.current_versions <> 1        THEN 'expected exactly 1 current version'
    WHEN coalesce(o.overlapping_pairs, 0) > 0 THEN 'validity windows overlap'
  END AS violation
FROM per_key p
LEFT JOIN overlaps o ON p.customer_unique_id = o.customer_unique_id
WHERE p.current_versions <> 1 OR coalesce(o.overlapping_pairs, 0) > 0;
