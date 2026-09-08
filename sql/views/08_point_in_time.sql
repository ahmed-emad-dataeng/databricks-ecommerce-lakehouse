-- THE SCD2 PAYOFF QUERY.
--
-- "What city and segment was this customer in AT THE TIME order X was placed?"
--
-- fact_order.customer_sk points at the customer VERSION that was current when
-- the order was placed, so this resolves history correctly. Joining on
-- customer_unique_id instead would restate every historical order with the
-- customer's present-day attributes -- which is exactly the bug SCD2 exists to
-- prevent. The `_current` columns below show that difference side by side.
CREATE OR REPLACE VIEW ${catalog}.gold.v_order_point_in_time
COMMENT 'Orders with as-at-purchase customer attributes vs present-day. Grain: one row per order.'
AS
SELECT
  fo.order_id,
  fo.order_purchase_timestamp,
  fo.order_total,
  fo.customer_unique_id,

  -- As at the time of the order (correct: joined via the version key)
  dc_hist.customer_city    AS city_at_order_time,
  dc_hist.customer_state   AS state_at_order_time,
  dc_hist.customer_segment AS segment_at_order_time,
  dc_hist.effective_from   AS version_effective_from,
  dc_hist.effective_to     AS version_effective_to,

  -- Present day (what a natural-key join would have given you)
  dc_now.customer_city     AS city_current,
  dc_now.customer_segment  AS segment_current,

  dc_hist.customer_city <> dc_now.customer_city AS customer_has_moved_since
FROM ${catalog}.gold.fact_order fo
LEFT JOIN ${catalog}.gold.dim_customer dc_hist
       ON fo.customer_sk = dc_hist.customer_sk
LEFT JOIN ${catalog}.gold.dim_customer dc_now
       ON fo.customer_unique_id = dc_now.customer_unique_id
      AND dc_now.is_current
      AND NOT dc_now.is_deleted;
