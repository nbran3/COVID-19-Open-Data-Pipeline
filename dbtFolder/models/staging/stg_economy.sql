{{
    config(
        materialized='view'
    )
}}


with 

source as (

    select * from {{ source('staging', 'economy') }}

),

economy as (

    select
        location_key,
        gdp_usd,
        gdp_per_capita_usd,
        human_capital_index,

    from source


)

select * from economy
