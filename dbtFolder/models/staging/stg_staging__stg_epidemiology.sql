{{
    config(
        materialized='view'
    )
}}

WITH source AS (
    SELECT * FROM {{ source('staging', 'stg_epidemiology') }}
),

epidemiology_non_casted AS (
    SELECT
        CAST(date AS DATE) AS casted_date,
        location_key,
        new_confirmed,
        new_deceased,
        new_recovered,
        new_tested,
        cumulative_confirmed,
        cumulative_deceased,
        cumulative_recovered,
        cumulative_tested
    FROM source
),

aggregate_epidemiology AS (
    SELECT
        location_key,
        casted_date,
        EXTRACT(MONTH FROM casted_date) AS casted_month,
        EXTRACT(DAY FROM casted_date) AS casted_day,
        EXTRACT(YEAR FROM casted_date) AS casted_year,
        DATE(EXTRACT(YEAR FROM casted_date), EXTRACT(MONTH FROM casted_date), 1) AS partition_date,
        new_confirmed,
        new_deceased,
        new_recovered,
        new_tested,
        cumulative_confirmed,
        cumulative_deceased,
        cumulative_recovered,
        cumulative_tested
        
    FROM epidemiology_non_casted
    ),


filtered_epideiology AS (
    SELECT *
    from aggregate_epidemiology
    WHERE casted_year >= 2019
    ORDER by location_key, casted_date
)

SELECT * FROM filtered_epideiology
