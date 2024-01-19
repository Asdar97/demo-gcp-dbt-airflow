{{
    config(
        materialized='table',
        unique_key=['rental_id']
    )
}}

WITH rental AS (

    SELECT * FROM {{ ref('rental') }}

),

payment AS (

    SELECT * FROM {{ ref('payment') }}

),

inventory AS (

    SELECT * FROM {{ ref('inventory') }}

),

base AS (
    SELECT
        r.rental_id
        ,r.rental_date
        ,r.inventory_id
        ,r.customer_id
        ,r.return_date
        ,r.staff_id
        ,i.film_id
        ,i.store_id
        ,DATE_DIFF(r.return_date, r.rental_date, DAY) AS rental_duration
        ,SUM(p.amount) AS amount
        ,MAX(p.payment_date) AS last_payment_date
    FROM rental r
        LEFT JOIN payment p ON r.rental_id = p.rental_id
        LEFT JOIN inventory i ON r.inventory_id = i.inventory_id
    GROUP BY 1,2,3,4,5,6,7,8
),

final AS (
    SELECT
        rental_id
        ,rental_date
        ,customer_id
        ,return_date
        ,staff_id
        ,film_id
        ,store_id
        ,rental_duration
        ,amount
        ,last_payment_date
    FROM base
)

SELECT * FROM final