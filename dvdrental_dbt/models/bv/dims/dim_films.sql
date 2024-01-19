{{
    config(
        materialized='table',
        unique_key=['film_id']
    )
}}

WITH film AS (

    SELECT * FROM {{ ref('film') }}

),

language AS (

    SELECT * FROM {{ ref('language') }}

),

category AS (

    SELECT * FROM {{ ref('category') }}

),

film_category AS (

    SELECT * FROM {{ ref('film_category') }}

),

actor AS (

    SELECT * FROM {{ ref('actor') }}

),

film_actor AS (

    SELECT * FROM {{ ref('film_actor') }}

),

category_tx AS (
    SELECT
        fc.film_id,
        ARRAY_AGG(c.name) AS categories
    FROM film_category fc
        LEFT JOIN category c ON fc.category_id = c.category_id
    GROUP BY 1
),

actor_tx AS (
    SELECT
        fa.film_id,
        ARRAY_AGG(CONCAT(a.first_name, ' ', a.last_name)) AS actors_involved,
        COUNT(a.first_name) AS total_actors_involved
    FROM film_actor fa
        LEFT JOIN actor a ON fa.actor_id = a.actor_id
    GROUP BY 1
),

base AS (
    SELECT
        f.film_id
        ,f.title
        ,f.description
        ,f.release_year
        ,f.rental_duration
        ,f.rental_rate
        ,f.length
        ,f.replacement_cost
        ,f.rating
        ,f.special_features
        ,l.name AS language
        ,ct.categories
        ,act.actors_involved
        ,act.total_actors_involved
    FROM film f
        LEFT JOIN language l ON f.language_id = l.language_id
        LEFT JOIN category_tx ct ON f.film_id = ct.film_id
        LEFT JOIN actor_tx act ON f.film_id = act.film_id
),

final AS (
    SELECT
        film_id
        ,title
        ,description
        ,release_year
        ,rental_duration
        ,rental_rate
        ,length
        ,replacement_cost
        ,rating
        ,special_features
        ,language
        ,categories
        ,actors_involved
        ,total_actors_involved
    FROM base
)

SELECT * FROM final

