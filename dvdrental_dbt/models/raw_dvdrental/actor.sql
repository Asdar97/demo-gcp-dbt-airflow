{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['actor_id']
    )
}}

WITH source AS (
    SELECT
        t.actor_id
        ,t.first_name
        ,t.last_name
        ,t.last_update
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source