{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['payment_id']
    )
}}

WITH source AS (
    SELECT
        t.payment_id
        ,t.customer_id
        ,t.staff_id
        ,t.rental_id
        ,t.amount
        ,t.payment_date
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source