{{ config(materialized='view') }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_transactions') }}
)

SELECT
    TO_HEX(MD5(CONCAT(merchant_name, '_', category))) AS merchant_id,
    merchant_name,
    category,
    AVG(merchant_lat) AS avg_merchant_lat,
    AVG(merchant_long) AS avg_merchant_long,
    COUNT(1) AS total_historical_transactions
FROM stg
GROUP BY
    merchant_name,
    category
