-- int_campaigns_day.sql
{{ config(materialized='table') }}

SELECT
    date_date,
    campaign_key,
    SUM(ads_cost) AS ads_cost,
    SUM(impression) AS ads_impression,
    SUM(click) AS ads_clicks
FROM {{ ref('int_campaigns') }}
GROUP BY
    date_date,
    campaign_key
ORDER BY date_date DESC




