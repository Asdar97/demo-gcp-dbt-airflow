{{
    config(
        materialized='table',
        unique_key=['customer_id']
    )
}}

WITH customer AS (

    SELECT * FROM {{ ref('customer') }}

),

final AS (
    SELECT
        customer_id
        ,store_id
        ,first_name
        ,last_name
        ,email
        ,address_id
        ,activebool
        ,create_date
        ,active
    FROM customer
)

SELECT * FROM final