{{
    config(
        materialized='table',
        unique_key=['actor_id']
    )
}}

WITH actor AS (

    SELECT * FROM {{ source('raw_dvdrental', 'actor') }}

),

film_actor AS (

    SELECT * FROM {{ source('raw_dvdrental', 'film_actor') }}

),

film AS (

    SELECT * FROM {{ source('raw_dvdrental', 'film') }}

),

film_tx AS (
    SELECT
        fa.actor_id
        ,ARRAY_AGG(f.title) AS films_involved
        ,COUNT(*) AS total_films_involved
    FROM film_actor fa
        LEFT JOIN film f ON fa.film_id = f.film_id
    GROUP BY 1
),

base AS (
    SELECT
        a.actor_id
        ,a.first_name
        ,a.last_name
        ,ft.films_involved
        ,ft.total_films_involved
    FROM actor a
        LEFT JOIN film_tx ft ON a.actor_id = ft.actor_id
),

final AS (
    SELECT
        actor_id
        ,first_name
        ,last_name
        ,films_involved
        ,total_films_involved
    FROM base
)

SELECT * FROM final

