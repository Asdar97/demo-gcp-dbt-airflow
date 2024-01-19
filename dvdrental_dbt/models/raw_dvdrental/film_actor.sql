{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['actor_id', 'film_id']
    )
}}

WITH source AS (
    SELECT
        t.actor_id
        ,t.film_id
        ,t.last_update
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source