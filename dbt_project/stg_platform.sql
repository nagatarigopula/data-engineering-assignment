{{ config(materialized='view') }}

select
    id as platform_id,
    "group" as platform_group,
    name,
    country
from {{ source('airflow_raw', 'platform') }}

