{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['country_id']
    )
}}

WITH source AS (
    SELECT
        t.country_id
        ,t.country
        ,t.last_update
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source