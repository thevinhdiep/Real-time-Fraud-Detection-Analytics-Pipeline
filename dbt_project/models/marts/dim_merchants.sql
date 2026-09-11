{{ config(materialized='view') }}

SELECT
    merchant_id,
    merchant_name,
    category,
    avg_merchant_lat,
    avg_merchant_long,
    total_historical_transactions,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('int_merchants_dedup') }}
