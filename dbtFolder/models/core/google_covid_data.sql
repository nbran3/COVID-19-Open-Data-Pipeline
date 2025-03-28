{{
    config(
        materialized='table'
        
    )
}}

WITH index_data AS (
    SELECT * 
    FROM {{ ref('stg_index')}} 
),

epidemiology_data AS(
    SELECT *
    FROM {{ ref('stg_partitioned_epidemiology')}}
),
hospitalizations_data as(
    SELECT *
    FROM {{ref('stg_partitioned_hospitalizations')}}
),
mobility_data as(
    SELECT *
    FROM {{ref('stg_partitioned_mobility')}}
),
vaccinations_data as(
    SELECT *
    FROM {{ref('stg_partitioned_vaccinations')}}
)

SELECT 
    i.*,
    epid.casted_date,
    epid.casted_month,
    epid.casted_day,
    epid.casted_year,
    epid.new_confirmed,
    epid.new_deceased,
    epid.new_recovered,
    epid.new_tested,
    epid.cumulative_confirmed,
    epid.cumulative_deceased,
    epid.cumulative_recovered,
    epid.cumulative_tested,
    mob.mobility_retail_and_recreation,
    mob.mobility_grocery_and_pharmacy,
    mob.mobility_parks,
    mob.mobility_transit_stations,
    mob.mobility_workplaces,
    mob.mobility_residential,
    hosp.new_hospitalized_patients,
    hosp.cumulative_hospitalized_patients,
    hosp.current_hospitalized_patients,
    hosp.new_intensive_care_patients,
    hosp.current_intensive_care_patients,
    hosp.cumulative_ventilator_patients,
    hosp.current_ventilator_patients,
    vacc.new_persons_vaccinated,
    vacc.cumulative_persons_vaccinated,
    vacc.new_persons_fully_vaccinated,
    vacc.cumulative_persons_fully_vaccinated,
    vacc.new_vaccine_doses_administered,
    vacc.cumulative_vaccine_doses_administered
FROM index_data i
FULL OUTER JOIN epidemiology_data epid 
    ON i.location_key = epid.location_key 
FULL OUTER JOIN mobility_data mob 
    ON i.location_key = mob.location_key 
    AND epid.casted_date = mob.casted_date
FULL OUTER JOIN hospitalizations_data hosp 
    ON i.location_key = hosp.location_key 
    AND epid.casted_date = hosp.casted_date
FULL OUTER JOIN vaccinations_data vacc 
    ON i.location_key = vacc.location_key 
    AND epid.casted_date = vacc.casted_date

WHERE country_name is not NULL
ORDER by country_name, casted_date





