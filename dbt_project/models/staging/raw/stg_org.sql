{{ config(materialized='view') }}

select
    id as org_id,
    name
from {{ source('airflow_raw', 'org') }}

