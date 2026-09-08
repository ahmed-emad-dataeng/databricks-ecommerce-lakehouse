-- Q4: What is the customer repeat-purchase rate?
-- Keyed on customer_unique_id, which is the entire point. See
-- 09_key_trap_demo.sql for what this number degrades to if keyed on customer_id.
CREATE OR REPLACE VIEW ${catalog}.gold.v_customer_metrics
COMMENT 'Per-customer orders and value. Grain: one row per customer_unique_id.'
AS
SELECT
  fo.customer_unique_id,
  count(DISTINCT fo.order_id)                   AS orders,
  sum(fo.order_total)                           AS lifetime_value,
  round(avg(fo.order_total), 2)                 AS avg_order_value,
  min(fo.order_purchase_timestamp)              AS first_order_ts,
  max(fo.order_purchase_timestamp)              AS last_order_ts,
  datediff(max(fo.order_purchase_timestamp), min(fo.order_purchase_timestamp))
                                                AS customer_lifespan_days,
  count(DISTINCT fo.order_id) > 1               AS is_repeat_customer
FROM ${catalog}.gold.fact_order fo
WHERE fo.customer_unique_id IS NOT NULL
GROUP BY fo.customer_unique_id;

CREATE OR REPLACE VIEW ${catalog}.gold.v_repeat_purchase_rate
COMMENT 'Headline repeat-purchase rate. Grain: one row (whole population).'
AS
SELECT
  count(*)                                                  AS customers,
  count_if(is_repeat_customer)                              AS repeat_customers,
  round(100.0 * count_if(is_repeat_customer) / count(*), 2) AS repeat_rate_pct,
  round(avg(orders), 3)                                     AS avg_orders_per_customer,
  round(avg(lifetime_value), 2)                             AS avg_lifetime_value
FROM ${catalog}.gold.v_customer_metrics;
