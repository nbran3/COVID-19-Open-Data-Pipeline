with 

source as (

    select * from {{ source('staging', 'stg_health') }}

),

health as (

    select
        location_key,
        life_expectancy,
        smoking_prevalence,
        diabetes_prevalence,
        infant_mortality_rate,
        adult_male_mortality_rate,
        adult_female_mortality_rate,
        pollution_mortality_rate,
        comorbidity_mortality_rate,
        hospital_beds_per_1000,
        nurses_per_1000,
        physicians_per_1000,
        health_expenditure_usd,
        out_of_pocket_health_expenditure_usd,
    from source

)

select * from health
