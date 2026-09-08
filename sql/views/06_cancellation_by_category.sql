-- Q6: Which product categories have the highest cancellation rate?
CREATE OR REPLACE VIEW ${catalog}.gold.v_cancellation_by_category
COMMENT 'Cancellation rate per category. Grain: one row per category.'
AS
SELECT
  coalesce(dp.category, 'unknown')            AS category,
  count(DISTINCT foi.order_id)                AS orders,
  count(DISTINCT CASE WHEN foi.is_cancelled THEN foi.order_id END)
                                              AS cancelled_orders,
  round(
    100.0 * count(DISTINCT CASE WHEN foi.is_cancelled THEN foi.order_id END)
    / nullif(count(DISTINCT foi.order_id), 0), 2
  )                                           AS cancellation_rate_pct,
  sum(CASE WHEN foi.is_cancelled THEN foi.item_revenue ELSE 0 END)
                                              AS cancelled_value
FROM ${catalog}.gold.fact_order_item foi
LEFT JOIN ${catalog}.gold.dim_product dp ON foi.product_sk = dp.product_sk
GROUP BY coalesce(dp.category, 'unknown')
HAVING count(DISTINCT foi.order_id) >= 30;  -- suppress noise from tiny categories
