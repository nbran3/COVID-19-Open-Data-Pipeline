{{
    config(
        materialized='view'
    )
}}


with 

source as (

    select * from {{ source('staging', 'mobility') }}

),

cast_mobility as (

    select
        cast(date as date) as casted_date,
        location_key,
        mobility_retail_and_recreation,
        mobility_grocery_and_pharmacy,
        mobility_parks,
        mobility_transit_stations,
        mobility_workplaces,
        mobility_residential,
        __index_level_0__

    from source

),

aggregate_mobility as (
    select
    location_key,
        casted_date,
        EXTRACT(MONTH FROM casted_date) AS casted_month,
        EXTRACT(DAY FROM casted_date) AS casted_day,
        EXTRACT(YEAR FROM casted_date) AS casted_year,
        DATE(EXTRACT(YEAR FROM casted_date), EXTRACT(MONTH FROM casted_date), 1) AS partition_date,
        mobility_retail_and_recreation,
        mobility_grocery_and_pharmacy,
        mobility_parks,
        mobility_transit_stations,
        mobility_workplaces,
        mobility_residential,

    from cast_mobility
),
filtered_mobility AS (
    SELECT *
    FROM aggregate_mobility
    WHERE casted_year >= 2019
    ORDER by location_key,casted_date
)

SELECT * FROM filtered_mobility

