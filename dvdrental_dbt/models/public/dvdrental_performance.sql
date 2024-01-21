{{
    config(
        materialized='view',
        unique_key=['Rental ID']
    )
}}

WITH fact_rental_payments AS (

    SELECT * FROM {{ ref('fact_rental_payments') }}

),

dim_customers AS (

    SELECT * FROM {{ ref('dim_customers') }}

),

dim_films AS (

    SELECT * FROM {{ ref('dim_films') }}

),

dim_locations AS (

    SELECT * FROM {{ ref('dim_locations') }}

),

dim_staffs AS (

    SELECT * FROM {{ ref('dim_staffs') }}

),

dim_stores AS (

    SELECT * FROM {{ ref('dim_stores') }}

),

base AS (
    SELECT
        rp.rental_id
        ,rp.rental_date
        ,rp.customer_id
        ,rp.return_date
        ,rp.staff_id
        ,rp.film_id
        ,rp.store_id
        ,rp.rental_duration
        ,rp.amount
        ,rp.last_payment_date
        -- ,c.first_name AS customer_first_name
        -- ,c.last_name AS customer_last_name
        ,CONCAT(c.first_name, ' ', c.last_name) AS customer_full_name
        -- ,c.email AS customer_email
        -- ,c.activebool AS customer_activebool
        ,c.create_date AS customer_create_date
        ,c.active AS customer_active
        ,f.title AS film_title
        ,f.description AS film_description
        ,f.release_year AS film_release_year
        ,f.language AS film_language
        ,f.rental_duration AS film_rental_duration
        ,f.rental_rate AS film_rental_rate
        ,f.length AS film_length
        ,f.replacement_cost AS film_replacement_cost
        ,f.rating AS film_rating
        ,f.special_features AS film_special_features
        ,f.actors_involved AS film_actors_involved
        ,f.total_actors_involved AS film_total_actors_involved
        ,f.categories AS film_categories
        ,s.manager_staff_id AS store_manager_staff_id
        ,CONCAT(l.address, ', ', l.address2) AS store_full_address
        ,l.district AS store_district
        ,l.city AS store_city
        ,l.country AS store_country
        ,l.postal_code AS store_postal_code
        -- ,l.phone AS store_phone
        ,CONCAT(st.first_name, '', st.last_name) AS staff_full_name
        -- ,st.email AS staff_email
        ,st.active AS staff_active
        ,st.username AS staff_username
    FROM fact_rental_payments rp
        LEFT JOIN dim_customers c ON rp.customer_id = c.customer_id
        LEFT JOIN dim_films f ON rp.film_id = f.film_id
        LEFT JOIN dim_stores s ON rp.store_id = s.store_id
        LEFT JOIN dim_locations l ON s.address_id = l.address_id
        LEFT JOIN dim_staffs st ON rp.staff_id = st.staff_id
),

final_rename AS (
    SELECT
        rental_id AS `Rental ID`
        ,rental_date AS `Rental Date`
        ,customer_id AS `Customer ID`
        ,return_date AS `Return Date`
        ,staff_id AS `Staff ID`
        ,film_id AS `Film ID`
        ,store_id AS `Store ID`
        ,rental_duration AS `Rental Duration`
        ,amount AS `Amount`
        ,last_payment_date AS `Last Payment Date`
        -- ,customer_first_name AS `Customer First Name`
        -- ,customer_last_name AS `Customer Last Name`
        ,customer_full_name AS `Customer Full Name`
        -- ,customer_email AS `Customer Email`
        -- ,customer_activebool AS `Customer Activebool`
        ,customer_create_date AS `Customer Create Date`
        ,customer_active AS `Customer Active`
        ,film_title AS `Film Title`
        ,film_description AS `Film Description`
        ,film_release_year AS `Film Release Year`
        ,film_language AS `Film Language`
        ,film_rental_duration AS `Film Rental Duration`
        ,film_rental_rate AS `Film Rental Rate`
        ,film_length AS `Film Length`
        ,film_replacement_cost AS `Film Replacement Cost`
        ,film_rating AS `Film Rating`
        ,film_special_features AS `Film Special Features`
        ,film_actors_involved AS `Film Actors Involved`
        ,film_total_actors_involved AS `Film Total Actors Involved`
        ,film_categories AS `Film Categories`
        ,store_manager_staff_id AS `Store Manager Staff ID`
        ,store_full_address AS `Store Full Address`
        ,store_district AS `Store District`
        ,store_city AS `Store City`
        ,store_country AS `Store Country`
        ,store_postal_code AS `Store Postal Code`
        -- ,store_phone AS `Store Phone`
        ,staff_full_name AS `Staff Full Name`
        -- ,staff_email AS `Staff Email`
        ,staff_active AS `Staff Active`
        ,staff_username AS `Staff Username`
    FROM base
)

SELECT * FROM final_rename

