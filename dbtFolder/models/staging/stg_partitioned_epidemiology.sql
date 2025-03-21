{{ config(
    materialized='table',
    partition_by={
        "field": "partition_date",
        "data_type": "DATE",
        
    },
    cluster_by=["location_key"]
) }}

SELECT * FROM {{ ref('stg_staging__stg_epidemiology') }}
