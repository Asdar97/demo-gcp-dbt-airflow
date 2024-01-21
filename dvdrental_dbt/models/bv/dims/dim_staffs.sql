{{
    config(
        materialized='table',
        unique_key=['staff_id']
    )
}}

WITH staff AS (

    SELECT * FROM {{ source('raw_dvdrental', 'staff') }}

),

final AS (
    SELECT
        staff_id
        ,first_name
        ,last_name
        ,email
        ,address_id
        ,active
        ,username
        -- ,password
    FROM staff
)

SELECT * FROM final