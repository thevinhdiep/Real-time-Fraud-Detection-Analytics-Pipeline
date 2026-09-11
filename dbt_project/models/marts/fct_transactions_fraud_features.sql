{{ config(materialized='view') }}

SELECT
    trans_num,
    customer_id,
    merchant_id,
    trans_timestamp,
    unix_time,
    amount,
    category,
    is_fraud,
    geo_distance_km,
    txn_freq_24h,
    amt_zscore_by_customer,
    trans_hour,
    day_of_week,
    is_weekend,
    is_night,
    ingested_at,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('int_transactions_enriched') }}
