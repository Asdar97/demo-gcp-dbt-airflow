{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['category_id']
    )
}}

WITH source AS (
    SELECT
        t.category_id
        ,t.name
        ,t.last_update
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source