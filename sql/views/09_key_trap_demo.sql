-- TRAP #1, DEMONSTRATED.
--
-- Olist issues a NEW customer_id for every order; only customer_unique_id is
-- stable across orders. Keying the customer dimension on customer_id therefore
-- makes every customer look brand new and drives repeat-purchase rate to ~0%.
--
-- This view computes the metric BOTH ways so the README can show the gap rather
-- than assert it. The `wrong_` number is what most Olist portfolio projects
-- publish without noticing.
CREATE OR REPLACE VIEW ${catalog}.gold.v_repeat_rate_key_comparison
COMMENT 'Repeat-purchase rate computed on the correct key vs the naive one. Grain: one row.'
AS
WITH correct_key AS (
  SELECT customer_unique_id AS k, count(DISTINCT order_id) AS orders
  FROM ${catalog}.gold.fact_order
  WHERE customer_unique_id IS NOT NULL
  GROUP BY customer_unique_id
),
naive_key AS (
  SELECT customer_id AS k, count(DISTINCT order_id) AS orders
  FROM ${catalog}.silver.orders
  GROUP BY customer_id
)
SELECT
  (SELECT count(*) FROM correct_key)                             AS customers_correct_key,
  (SELECT count(*) FROM naive_key)                               AS customers_naive_key,
  (SELECT round(100.0 * count_if(orders > 1) / count(*), 2) FROM correct_key)
                                                                 AS repeat_rate_pct_correct,
  (SELECT round(100.0 * count_if(orders > 1) / count(*), 2) FROM naive_key)
                                                                 AS repeat_rate_pct_naive;
