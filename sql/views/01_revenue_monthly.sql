-- Q1: How does revenue change over time?
-- Reads fact_order_item because that is the revenue grain. Cancelled lines are
-- excluded from revenue but still reported, so the dashboard can show both.
CREATE OR REPLACE VIEW ${catalog}.gold.v_revenue_monthly
COMMENT 'Monthly revenue, orders and AOV. Grain: one row per calendar month.'
AS
SELECT
  d.year_month,
  d.year,
  d.month,
  count(DISTINCT foi.order_id)                                         AS orders,
  sum(CASE WHEN NOT foi.is_cancelled THEN foi.item_revenue ELSE 0 END) AS revenue,
  sum(CASE WHEN foi.is_cancelled THEN foi.item_revenue ELSE 0 END)     AS cancelled_value,
  count(*)                                                             AS order_lines,
  round(
    sum(CASE WHEN NOT foi.is_cancelled THEN foi.item_revenue ELSE 0 END)
    / nullif(count(DISTINCT foi.order_id), 0), 2
  )                                                                    AS avg_order_value
FROM ${catalog}.gold.fact_order_item foi
JOIN ${catalog}.gold.dim_date d ON foi.order_date_sk = d.date_sk
GROUP BY d.year_month, d.year, d.month;
