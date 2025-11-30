with orders_daily as (
    select
        date,
        listing_id,
        orders,
        timestamp
    from {{ source('raw', 'orders_daily') }}
)

select
    date,
    listing_id,
    orders,
    timestamp,
    sum(orders) over (
        partition by listing_id
        order by date
        rows between unbounded preceding and current row
    ) as aggregated_orders
from orders_daily

