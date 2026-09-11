{{ config(materialized='view') }}

WITH stg AS (
    SELECT * FROM {{ ref('stg_transactions') }}
),

ranked_customers AS (
    SELECT
        TO_HEX(MD5(CAST(cc_num AS STRING))) AS customer_id,
        cc_num,
        first_name,
        last_name,
        gender,
        street,
        city,
        state,
        zip_code,
        customer_lat,
        customer_long,
        city_population,
        job_title,
        date_of_birth,
        DATE_DIFF(CURRENT_DATE(), date_of_birth, YEAR) AS customer_age,
        ROW_NUMBER() OVER (PARTITION BY cc_num ORDER BY trans_timestamp DESC) AS rn
    FROM stg
)

SELECT
    customer_id,
    cc_num,
    first_name,
    last_name,
    gender,
    street,
    city,
    state,
    zip_code,
    customer_lat,
    customer_long,
    city_population,
    job_title,
    date_of_birth,
    customer_age
FROM ranked_customers
WHERE rn = 1
