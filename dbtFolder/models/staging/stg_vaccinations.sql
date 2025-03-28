{{
    config(
        materialized='view'
    )
}}


with 

source as (

    select * from {{ source('staging', 'vaccinations') }}

),

cast_vaccinations as (

    select
        CAST(date AS DATE) AS casted_date,
        location_key,
        new_persons_vaccinated,
        cumulative_persons_vaccinated,
        new_persons_fully_vaccinated,
        cumulative_persons_fully_vaccinated,
        new_vaccine_doses_administered,
        cumulative_vaccine_doses_administered,

    from source

),
    aggregated_vaccinations AS (
        SELECT 
        location_key,
        casted_date,
        EXTRACT(MONTH FROM casted_date) AS casted_month,
        EXTRACT(DAY FROM casted_date) AS casted_day,
        EXTRACT(YEAR FROM casted_date) AS casted_year,
        DATE(EXTRACT(YEAR FROM casted_date), EXTRACT(MONTH FROM casted_date), 1) AS partition_date,
        new_persons_vaccinated,
        cumulative_persons_vaccinated,
        new_persons_fully_vaccinated,
        cumulative_persons_fully_vaccinated,
        new_vaccine_doses_administered,
        cumulative_vaccine_doses_administered

        from cast_vaccinations

    ),


filtered_vaccinations AS (
    SELECT *
    FROM aggregated_vaccinations
    WHERE casted_year >= 2019
    ORDER by location_key, casted_date
)


select * from filtered_vaccinations
