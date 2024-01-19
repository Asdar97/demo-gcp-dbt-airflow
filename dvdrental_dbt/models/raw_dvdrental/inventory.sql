{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['inventory_id']
    )
}}

WITH source AS (
    SELECT
        t.inventory_id
        ,t.film_id
        ,t.store_id
        ,t.last_update
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source