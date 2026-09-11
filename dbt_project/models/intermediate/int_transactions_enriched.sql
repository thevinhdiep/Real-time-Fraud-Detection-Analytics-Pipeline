{{ config(materialized='view') }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

cust AS (
    SELECT * FROM {{ ref('int_customers_dedup') }}
),

merch AS (
    SELECT * FROM {{ ref('int_merchants_dedup') }}
),

enriched AS (
    SELECT
        t.trans_num,
        c.customer_id,
        m.merchant_id,
        t.trans_timestamp,
        t.unix_time,
        t.amount,
        t.category,
        t.is_fraud,
        t.customer_lat,
        t.customer_long,
        t.merchant_lat,
        t.merchant_long,

        -- ===== Feature 1: Geo-distance (Haversine) =====
        ROUND(
            ST_DISTANCE(
                ST_GEOGPOINT(t.customer_long, t.customer_lat),
                ST_GEOGPOINT(t.merchant_long, t.merchant_lat)
            ) / 1000.0,
            2
        ) AS geo_distance_km,

        -- ===== Feature 2: Transaction frequency in last 24h =====
        -- Count transactions by same cc_num within 86400 seconds before this transaction
        COUNT(*) OVER (
            PARTITION BY t.cc_num
            ORDER BY t.unix_time
            RANGE BETWEEN 86400 PRECEDING AND 1 PRECEDING
        ) AS txn_freq_24h,

        -- ===== Feature 3: Amount Z-score per customer =====
        -- How unusual is this transaction amount vs customer's historical spending
        ROUND(
            SAFE_DIVIDE(
                t.amount - AVG(t.amount) OVER (PARTITION BY t.cc_num),
                NULLIF(STDDEV(t.amount) OVER (PARTITION BY t.cc_num), 0)
            ),
            4
        ) AS amt_zscore_by_customer,

        -- ===== Temporal Features =====
        EXTRACT(HOUR FROM t.trans_timestamp) AS trans_hour,
        EXTRACT(DAYOFWEEK FROM t.trans_timestamp) AS day_of_week,
        CASE WHEN EXTRACT(DAYOFWEEK FROM t.trans_timestamp) IN (1, 7) THEN 1 ELSE 0 END AS is_weekend,
        CASE WHEN EXTRACT(HOUR FROM t.trans_timestamp) BETWEEN 0 AND 5 THEN 1 ELSE 0 END AS is_night,

        t.ingested_at
    FROM stg t
    INNER JOIN cust c ON t.cc_num = c.cc_num
    INNER JOIN merch m ON t.merchant_name = m.merchant_name AND t.category = m.category
)

SELECT * FROM enriched
