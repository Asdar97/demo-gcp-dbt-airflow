{{
    config(
        materialized='table',
        unique_key=['address_id']
    )
}}

WITH address AS (

    SELECT * FROM {{ ref('address') }}

),

city AS (

    SELECT * FROM {{ ref('city') }}

),

country AS (

    SELECT * FROM {{ ref('country') }}

),

base AS (
    SELECT
        a.address_id
        ,a.address
        ,a.address2
        ,a.district
        ,a.postal_code
        ,a.phone
        ,ci.city
        ,co.country
    FROM address a
        LEFT JOIN city ci ON a.city_id = ci.city_id
        LEFT JOIN country co ON ci.country_id = co.country_id
),

final AS (
    SELECT
        address_id
        ,address
        ,address2
        ,district
        ,postal_code
        ,phone
        ,city
        ,country
    FROM base
)

SELECT * FROM final



