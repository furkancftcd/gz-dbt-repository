{{ config(materialized='table') }}

SELECT
  date_date,
  COUNT(DISTINCT orders_id)              AS nb_transactions,
  SUM(revenue)                           AS revenue,
  ROUND(SAFE_DIVIDE(SUM(revenue),
        COUNT(DISTINCT orders_id)), 2)   AS average_basket,
  SUM(operational_margin)                AS operational_margin,
  SUM(purchase_cost)                     AS purchase_cost,
  SUM(shipping_fee)                      AS shipping_fee,
  SUM(log_cost + ship_cost)              AS logistics_cost,
  SUM(quantity)                          AS quantity
FROM {{ ref('int_orders_operational') }}
GROUP BY date_date
ORDER BY date_date DESC




