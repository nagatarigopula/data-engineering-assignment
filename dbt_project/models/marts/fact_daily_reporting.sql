{{ config(materialized='table') }}

with base as (
    select
        od.date,
        od.listing_id,

        -- listing / outlet / org / platform keys
        l.outlet_id,
        l.platform_id,
        o.org_id,

        -- metrics
        od.orders,
        od.aggregated_orders,
        r.avg_rating,
        r.cnt_ratings,
        rk.avg_rank

    from {{ ref('int_daily_orders') }} od
    left join {{ ref('int_listings') }}  l  on od.listing_id = 
l.listing_id
    left join {{ ref('int_outlets') }}   o  on l.outlet_id  = o.outlet_id
    left join {{ ref('int_orgs') }}      org on o.org_id    = org.org_id
    left join {{ ref('int_platforms') }} p on l.platform_id = 
p.platform_id
    left join {{ ref('int_ratings') }}   r on od.date = r.date and 
od.listing_id = r.listing_id
    left join {{ ref('int_rank') }}      rk on od.date = rk.date and 
od.listing_id = rk.listing_id
),

weather_daily as (
    select
        outlet_id,
        date,
        avg_temperature_2m,
        avg_relative_humidity_2m,
        avg_wind_speed_10m
    from {{ ref('int_weather') }}
)

select
    b.date,
    b.listing_id,
    b.outlet_id,
    b.org_id,
    b.platform_id,

    -- orders + ratings + rank
    b.orders,
    b.aggregated_orders,
    b.avg_rating,
    b.cnt_ratings,
    b.avg_rank,

    -- weather
    wd.avg_temperature_2m,
    wd.avg_relative_humidity_2m,
    wd.avg_wind_speed_10m

from base b
left join weather_daily wd
    on b.outlet_id = wd.outlet_id
   and b.date = wd.date

order by b.date, b.listing_id

