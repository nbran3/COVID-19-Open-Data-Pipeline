{{
    config(
        materialized='view'
    )
}}


with 

source as (

    select * from {{ source('staging', 'index') }}

),

index as (

    select *

    from source


)

select * from index
