{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['address_id']
    )
}}

WITH source AS (
    SELECT
        t.address_id
        ,t.address
        ,t.address2
        ,t.district
        ,t.city_id
        ,t.postal_code
        ,t.phone
        ,t.last_update
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source