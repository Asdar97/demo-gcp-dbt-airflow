{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['customer_id']
    )
}}

WITH source AS (
    SELECT
        t.customer_id
        ,t.store_id
        ,t.first_name
        ,t.last_name
        ,t.email
        ,t.address_id
        ,t.activebool
        ,t.create_date
        ,t.last_update
        ,t.active
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source