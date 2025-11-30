{{ config(materialized='view') }}

select
    org_id,
    name as org_name
from {{ ref('stg_org') }}

