{{ config(materialized='table') }}

SELECT
  DATE_TRUNC(COALESCE(d.date_date, c.date_date), MONTH) AS datemonth,

  SUM(COALESCE(d.operational_margin, 0) - COALESCE(c.ads_cost, 0)) AS ads_margin,
  ROUND(
    SUM(COALESCE(d.revenue, 0)) / NULLIF(SUM(COALESCE(d.nb_transactions, 0)), 0),
    2
  ) AS average_basket,

  SUM(COALESCE(d.operational_margin, 0)) AS operational_margin,
  SUM(COALESCE(c.ads_cost, 0))           AS ads_cost,
  SUM(COALESCE(c.ads_impression, 0))     AS ads_impression,
  SUM(COALESCE(c.ads_clicks, 0))         AS ads_clicks,

  SUM(COALESCE(d.quantity, 0))           AS quantity,
  SUM(COALESCE(d.revenue, 0))            AS revenue

FROM {{ ref('finance_campaigns_day') }} d
FULL OUTER JOIN {{ ref('int_campaigns_day') }} c
  ON d.date_date = c.date_date

GROUP BY datemonth
ORDER BY datemonth DESC


