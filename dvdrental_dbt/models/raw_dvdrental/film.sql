{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['film_id']
    )
}}

WITH source AS (
    SELECT
        t.film_id
        ,t.title
        ,t.description
        ,t.release_year
        ,t.language_id
        ,t.rental_duration
        ,t.rental_rate
        ,t.length
        ,t.replacement_cost
        ,t.rating
        ,t.last_update
        ,t.special_features
        ,t.fulltext
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source