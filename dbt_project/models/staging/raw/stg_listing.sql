{{ config(materialized='view') }}

select
    id as listing_id,
    outlet_id,
    platform_id,
    timestamp
from {{ source('airflow_raw', 'listing') }}

