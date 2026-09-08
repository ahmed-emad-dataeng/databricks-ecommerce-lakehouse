-- Q3: Which sellers perform best and worst?
-- Monthly rank per seller via a window, so "who led in March 2018" is
-- answerable, not merely "who leads overall".
CREATE OR REPLACE VIEW ${catalog}.gold.v_seller_monthly_performance
COMMENT 'Seller revenue by month with in-month rank. Grain: one row per (seller, month).'
AS
SELECT
  ds.seller_id,
  ds.seller_state,
  d.year_month,
  sum(foi.item_revenue)                        AS revenue,
  count(DISTINCT foi.order_id)                 AS orders,
  count(*)                                     AS order_lines,
  rank() OVER (PARTITION BY d.year_month ORDER BY sum(foi.item_revenue) DESC)
                                               AS rank_in_month,
  round(avg(foi.item_price), 2)                AS avg_item_price
FROM ${catalog}.gold.fact_order_item foi
JOIN ${catalog}.gold.dim_seller ds ON foi.seller_sk = ds.seller_sk
JOIN ${catalog}.gold.dim_date d    ON foi.order_date_sk = d.date_sk
WHERE NOT foi.is_cancelled
GROUP BY ds.seller_id, ds.seller_state, d.year_month;
