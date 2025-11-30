{{ config(materialized='view') }}

select
    date,
    listing_id,
    orders,
    timestamp
from {{ source('airflow_raw', 'orders_daily') }}

