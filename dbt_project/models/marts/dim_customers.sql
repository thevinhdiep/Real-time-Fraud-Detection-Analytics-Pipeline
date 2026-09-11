{{ config(materialized='view') }}

SELECT
    customer_id,
    cc_num,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
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
    customer_age,
    CURRENT_TIMESTAMP() AS dbt_updated_at
FROM {{ ref('int_customers_dedup') }}
