{{ config(materialized='view') }}

select
    listing_id,
    date,
    timestamp as ts,
    is_online,
    rank
from {{ source('airflow_raw', 'rank') }}

