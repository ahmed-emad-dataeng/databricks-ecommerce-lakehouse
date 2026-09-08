-- Q5: How long do shipments take, and how often are they late?
-- delivery_days is NULL for undelivered orders, so AVG ignores open orders
-- instead of scoring them as instant deliveries.
CREATE OR REPLACE VIEW ${catalog}.gold.v_delivery_performance
COMMENT 'Delivery SLA by month and state. Grain: one row per (month, customer_state).'
AS
SELECT
  d.year_month,
  dc.customer_state,
  count(*)                                   AS orders,
  count_if(fo.is_delivered)                  AS delivered_orders,
  round(avg(fo.delivery_days), 2)            AS avg_delivery_days,
  percentile_approx(fo.delivery_days, 0.5)   AS median_delivery_days,
  percentile_approx(fo.delivery_days, 0.95)  AS p95_delivery_days,
  count_if(fo.is_late)                       AS late_orders,
  round(100.0 * count_if(fo.is_late) / nullif(count_if(fo.is_delivered), 0), 2)
                                             AS late_rate_pct,
  round(avg(fo.delivery_delay_days), 2)      AS avg_delay_vs_estimate,
  round(avg(fo.approval_lag_hours), 2)       AS avg_approval_lag_hours
FROM ${catalog}.gold.fact_order fo
JOIN ${catalog}.gold.dim_date d            ON fo.order_date_sk = d.date_sk
LEFT JOIN ${catalog}.gold.dim_customer dc  ON fo.customer_sk = dc.customer_sk
GROUP BY d.year_month, dc.customer_state;
