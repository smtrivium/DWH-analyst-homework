with completed_orders as (
    select
        hc.customer_pk,
        count(distinct ho.order_pk) as completed_orders_count
    from {{ ref('hub_customer') }} hc
    join {{ ref('link_customer_order') }} lco on hc.customer_pk = lco.customer_pk
    join {{ ref('hub_order') }} ho on lco.order_pk = ho.order_pk
    join {{ ref('sat_order') }} sod on ho.order_pk = sod.order_pk
    where sod.status = 'completed'
    group by 1
)

select
    co.customer_pk,
    sc.first_name,
    sc.last_name,
    sc.email,
    co.completed_orders_count
from completed_orders co
join {{ ref('stg_customers') }} sc on co.customer_pk = sc.customer_pk
order by co.completed_orders_count desc