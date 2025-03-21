{{
    config(
        materialized='table'
        
    )
}}

WITH index_data AS (
    SELECT * 
    FROM {{ ref('stg_staging__index')}} 
),

economy_data AS (
    SELECT * 
    FROM {{ ref('stg_staging__stg_economy') }} 
),
geography_data as(
    SELECT *
    FROM {{ref('stg_staging__stg_geography')}}
),
health_data as(
    SELECT *
    FROM {{ref('stg_staging__stg_health')}}
),
demographics_data as(
    SELECT *
    FROM {{ref('stg_staging__stg_demographics')}}
)

SELECT 
    i.*,
    econ.gdp_usd,
    econ.gdp_per_capita_usd,
    econ.human_capital_index,
    geo.openstreetmap_id,
    geo.latitude,
    geo.longitude,
    geo.elevation_m,
    geo.area_sq_km,
    geo.area_rural_sq_km,
    geo.area_urban_sq_km,
    hea.life_expectancy,
    hea.smoking_prevalence,
    hea.diabetes_prevalence,
    hea.infant_mortality_rate,
    hea.adult_male_mortality_rate,
    hea.adult_female_mortality_rate,
    hea.pollution_mortality_rate,
    hea.comorbidity_mortality_rate,
    hea.hospital_beds_per_1000,
    hea.nurses_per_1000,
    hea.physicians_per_1000,
    hea.health_expenditure_usd,
    hea.out_of_pocket_health_expenditure_usd,
    demo.population,
    demo.population_male,
    demo.population_female,
    demo.population_rural,
    demo.population_urban,
    demo.population_largest_city,
    demo.population_clustered,
    demo.population_density,
    demo.population_age_00_19,
    demo.population_age_20_39,
    demo.population_age_40_59,
    demo.population_age_older_than_60
FROM index_data i
FULL OUTER JOIN economy_data econ 
    on i.location_key = econ.location_key 
FULL OUTER JOIN geography_data geo 
    on i.location_key = geo.location_key 
FULL OUTER JOIN health_data hea 
    on i.location_key = hea.location_key 
FULL OUTER JOIN demographics_data demo
    on i.location_key = demo.location_key 
WHERE country_name is not NULL
ORDER by country_name