{{ config(materialized='view') }}

WITH raw_source AS (
    SELECT
        trans_num,
        trans_date_trans_time,
        unix_time,
        cc_num,
        merchant,
        category,
        amt,
        first AS first_name,
        last AS last_name,
        gender,
        street,
        city,
        state,
        zip,
        lat AS cust_lat,
        long AS cust_long,
        city_pop,
        job,
        dob,
        merch_lat,
        merch_long,
        is_fraud,
        ingested_at
    FROM `xombank-fraud-analytics.xombank_dw.raw_transactions`
),

cleaned_and_typed AS (
    SELECT
        CAST(trans_num AS STRING) AS trans_num,
        PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', trans_date_trans_time) AS trans_timestamp,
        CAST(unix_time AS INT64) AS unix_time,
        CAST(cc_num AS INT64) AS cc_num,
        TRIM(CAST(merchant AS STRING)) AS merchant_name,
        TRIM(LOWER(CAST(category AS STRING))) AS category,
        CAST(amt AS FLOAT64) AS amount,
        TRIM(CAST(first_name AS STRING)) AS first_name,
        TRIM(CAST(last_name AS STRING)) AS last_name,
        UPPER(TRIM(CAST(gender AS STRING))) AS gender,
        TRIM(CAST(street AS STRING)) AS street,
        TRIM(CAST(city AS STRING)) AS city,
        UPPER(TRIM(CAST(state AS STRING))) AS state,
        CAST(zip AS INT64) AS zip_code,
        CAST(cust_lat AS FLOAT64) AS customer_lat,
        CAST(cust_long AS FLOAT64) AS customer_long,
        CAST(city_pop AS INT64) AS city_population,
        TRIM(CAST(job AS STRING)) AS job_title,
        PARSE_DATE('%Y-%m-%d', dob) AS date_of_birth,
        CAST(merch_lat AS FLOAT64) AS merchant_lat,
        CAST(merch_long AS FLOAT64) AS merchant_long,
        CAST(is_fraud AS INT64) AS is_fraud,
        ingested_at
    FROM raw_source
)

SELECT *
FROM cleaned_and_typed
QUALIFY ROW_NUMBER() OVER(PARTITION BY trans_num ORDER BY ingested_at DESC) = 1
