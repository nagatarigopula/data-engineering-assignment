{{ config(materialized='view') }}

select
    listing_id,
    order_id,
    placed_at,
    status
from {{ source('airflow_raw', 'orders') }}

