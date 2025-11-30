{{ config(materialized='view') }}

select
    platform_id,
    platform_group,
    name as platform_name,
    country
from {{ ref('stg_platform') }}

