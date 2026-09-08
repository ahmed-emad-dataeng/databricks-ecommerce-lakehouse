-- Q2: Which product categories generate the most revenue?
-- Carries the cumulative share so "top N categories = X% of revenue" needs no
-- second query.
CREATE OR REPLACE VIEW ${catalog}.gold.v_revenue_by_category
COMMENT 'Category performance with revenue share and rank. Grain: one row per category.'
AS
WITH by_category AS (
  SELECT
    coalesce(dp.category, 'unknown')  AS category,
    sum(foi.item_revenue)             AS revenue,
    count(*)                          AS order_lines,
    count(DISTINCT foi.order_id)      AS orders,
    round(avg(foi.item_price), 2)     AS avg_item_price
  FROM ${catalog}.gold.fact_order_item foi
  LEFT JOIN ${catalog}.gold.dim_product dp ON foi.product_sk = dp.product_sk
  WHERE NOT foi.is_cancelled
  GROUP BY coalesce(dp.category, 'unknown')
)
SELECT
  category,
  revenue,
  orders,
  order_lines,
  avg_item_price,
  rank() OVER (ORDER BY revenue DESC)                              AS revenue_rank,
  round(100.0 * revenue / sum(revenue) OVER (), 2)                 AS pct_of_revenue,
  round(
    100.0 * sum(revenue) OVER (ORDER BY revenue DESC) / sum(revenue) OVER (), 2
  )                                                                AS cumulative_pct
FROM by_category;
