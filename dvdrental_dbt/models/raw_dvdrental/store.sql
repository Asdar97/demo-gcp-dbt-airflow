{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['store_id']
    )
}}

WITH source AS (
    SELECT
        t.store_id
        ,t.manager_staff_id
        ,t.address_id
        ,t.last_update
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source