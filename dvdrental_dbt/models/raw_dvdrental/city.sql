{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['city_id']
    )
}}

WITH source AS (
    SELECT
        t.city_id
        ,t.city
        ,t.country_id
        ,t.last_update
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source