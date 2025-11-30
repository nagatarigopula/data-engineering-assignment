{{ config(materialized='view') }}

select
    outlet_id,
    org_id,
    name as outlet_name,
    latitude,
    longitude,
    timestamp as outlet_timestamp
from {{ ref('stg_outlet') }}

