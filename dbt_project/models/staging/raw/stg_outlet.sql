{{ config(materialized='view') }}

select
    id as outlet_id,
    org_id,
    name,
    latitude,
    longitude,
    timestamp
from {{ source('airflow_raw', 'outlet') }}

