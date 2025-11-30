{{ config(materialized='view') }}

select
    listing_id,
    outlet_id,
    platform_id
from {{ ref('stg_listing') }}

