{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['staff_id']
    )
}}

WITH source AS (
    SELECT
        t.staff_id
        ,t.first_name
        ,t.last_name
        ,t.address_id
        ,t.email
        ,t.store_id
        ,t.active
        ,t.username
        ,t.password
        ,t.last_update
        ,t.picture
        ,CURRENT_TIMESTAMP() AS LOAD_TIME
    FROM {{ var("database") }}.{{ var("table") }} t
)

SELECT * FROM source