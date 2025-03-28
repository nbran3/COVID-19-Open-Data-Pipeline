with 

source as (

    select * from {{ source('staging', 'demographics') }}

),

renamed as (

    select
        location_key,
        population,
        population_male,
        population_female,
        population_rural,
        population_urban,
        population_largest_city,
        population_clustered,
        population_density,
        human_development_index,
    (population_age_00_09 + population_age_10_19) as population_age_00_19,
    (population_age_20_29 + population_age_30_39) as population_age_20_39,
    (population_age_40_49 + population_age_50_59) as population_age_40_59,
    (population_age_60_69 + population_age_70_79 + population_age_80_and_older) as population_age_older_than_60
    from source
    ORDER by location_key

),

cast_demographics as (
    SELECT
    location_key,
    cast(population as int) as population,
    cast(population_male as int) as population_male,
    cast(population_female as int) as population_female,
    cast(population_rural as int) as population_rural,
    cast(population_urban as int) as population_urban,
    cast(population_largest_city as int) as population_largest_city,
    cast(population_clustered as int) as population_clustered,
    population_density,
    cast(population_age_00_19 as int) as population_age_00_19,
    cast(population_age_20_39 as int) as population_age_20_39,
    cast(population_age_40_59 as int) as population_age_40_59,
    cast(population_age_older_than_60 as int) as population_age_older_than_60
    FROM renamed

)

select * from cast_demographics
