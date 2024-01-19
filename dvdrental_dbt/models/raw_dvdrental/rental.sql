{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['rental_id']
    )
}}

WITH source AS (
    SELECT
        t.rental_id
        ,t.rental_date
        ,t.inventory_id
        ,t.customer_id
        ,t.return_date
        ,t.staff_id
        ,t.last_update
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source