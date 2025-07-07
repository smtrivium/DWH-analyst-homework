with weekly_orders as (
    select
        date_trunc('week', order_date) as week_start,
        count(*) as order_count
    from {{ ref('sat_order') }}
    group by 1
    order by 1
)

select
    week_start,
    order_count,
    lag(order_count) over (order by week_start) as prev_week_count,
    order_count - lag(order_count) over (order by week_start) as week_over_week_change,
    (order_count::float / lag(order_count) over (order by week_start) - 1) * 100 as week_over_week_change_pct
from weekly_orders