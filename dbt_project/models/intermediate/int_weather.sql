{{ config(materialized='view') }}

-- Aggregate hourly weather data into daily per outlet
with w as (
    select
        outlet_id,
        cast(timestamp as date) as date,
        temperature_2m,
        relative_humidity_2m,
        wind_speed_10m
    from {{ source('raw', 'weather') }}
)

select
    outlet_id,
    date,
    avg(temperature_2m)       as avg_temperature_2m,
    avg(relative_humidity_2m) as avg_relative_humidity_2m,
    avg(wind_speed_10m)       as avg_wind_speed_10m
from w
group by 1, 2
order by 1, 2

