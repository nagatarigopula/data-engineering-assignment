{{ config(materialized='view') }}

select
    date,
    listing_id,
    cnt_ratings,
    avg_rating
from {{ ref('stg_ratings_agg') }}

