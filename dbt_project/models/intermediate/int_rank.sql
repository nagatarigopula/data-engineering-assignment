{{ config(materialized='view') }}

select
    date,
    listing_id,
    avg(rank) as avg_rank
from {{ ref('stg_rank') }}
group by 1,2

