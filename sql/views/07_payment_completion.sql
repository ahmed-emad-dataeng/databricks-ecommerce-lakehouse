-- Q7 (wave 2): How do payment methods affect order completion?
-- Reads fact_payment at PAYMENT grain. Note the deliberate absence of any join
-- to fact_order totals -- that is what would fan out. See 10_fanout_demo.sql.
CREATE OR REPLACE VIEW ${catalog}.gold.v_payment_method_completion
COMMENT 'Completion and installment behaviour per payment type. Grain: one row per payment_type.'
AS
SELECT
  fp.payment_type,
  count(*)                                                 AS payment_records,
  count(DISTINCT fp.order_id)                              AS orders,
  sum(fp.payment_value)                                    AS total_paid,
  round(avg(fp.payment_value), 2)                          AS avg_payment_value,
  round(avg(fp.payment_installments), 2)                   AS avg_installments,
  count(DISTINCT CASE WHEN fp.order_completed THEN fp.order_id END)
                                                           AS completed_orders,
  round(
    100.0 * count(DISTINCT CASE WHEN fp.order_completed THEN fp.order_id END)
    / nullif(count(DISTINCT fp.order_id), 0), 2
  )                                                        AS completion_rate_pct
FROM ${catalog}.gold.fact_payment fp
GROUP BY fp.payment_type;
