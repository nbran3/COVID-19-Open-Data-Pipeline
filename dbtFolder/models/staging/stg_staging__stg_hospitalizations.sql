WITH source AS (
    SELECT * FROM {{ source('staging', 'stg_hospitalizations') }}
),

casted_hospitalizations AS (
    SELECT
        CAST(date AS DATE) AS casted_date,
        location_key,
        new_hospitalized_patients,
        cumulative_hospitalized_patients,
        current_hospitalized_patients,
        new_intensive_care_patients,
        cumulative_intensive_care_patients,
        current_intensive_care_patients,
        new_ventilator_patients,
        cumulative_ventilator_patients,
        current_ventilator_patients  
    FROM source
),

aggregate_hospitalizations AS (
    SELECT
        location_key,
        casted_date,
        EXTRACT(MONTH FROM casted_date) AS casted_month,
        EXTRACT(DAY FROM casted_date) AS casted_day,
        EXTRACT(YEAR FROM casted_date) AS casted_year,
        DATE(EXTRACT(YEAR FROM casted_date), EXTRACT(MONTH FROM casted_date), 1) AS partition_date,
        new_hospitalized_patients,
        cumulative_hospitalized_patients,
        current_hospitalized_patients,
        new_intensive_care_patients,
        cumulative_intensive_care_patients,
        current_intensive_care_patients,
        new_ventilator_patients,
        cumulative_ventilator_patients,
        current_ventilator_patients 
    FROM casted_hospitalizations
 
),


filtered_hospitalizations AS (
    SELECT *
    FROM aggregate_hospitalizations
    WHERE casted_year >= 2019
    ORDER by location_key, casted_date
)

SELECT * FROM filtered_hospitalizations

