{{ config(materialized='view') }}

select
    date,
    listing_id,
    cnt_ratings,
    avg_rating
from {{ source('airflow_raw', 'ratings_agg') }}

