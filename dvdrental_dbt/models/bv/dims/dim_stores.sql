{{
    config(
        materialized='table',
        unique_key=['store_id']
    )
}}

WITH store AS (

    SELECT * FROM {{ ref('store') }}

),

final AS (
    SELECT
        store_id
        ,manager_staff_id
        ,address_id
    FROM store
)

SELECT * FROM final