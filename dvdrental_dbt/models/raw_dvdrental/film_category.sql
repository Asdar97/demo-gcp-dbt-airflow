{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['film_id', 'category_id']
    )
}}

WITH source AS (
    SELECT
        t.film_id
        ,t.category_id
        ,t.last_update
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source