with 

source as (

    select * from {{ source('staging', 'geography') }}

),

geography as (

    select
        location_key,
        openstreetmap_id,
        latitude,
        longitude,
        elevation_m,
        area_sq_km,
        area_rural_sq_km,
        area_urban_sq_km,

    from source

)

select * from geography
