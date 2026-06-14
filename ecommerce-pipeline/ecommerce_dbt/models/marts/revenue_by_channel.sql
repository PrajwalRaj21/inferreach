-- models/marts/revenue_by_channel.sql
-- Revenue aggregated by acquisition channel — powers the Looker Studio dashboard

with orders as (
    select * from {{ ref('fct_orders') }}
    where is_completed = true
)

select
    channel,
    order_date,
    count(distinct order_id)       as total_orders,
    count(distinct customer_id)    as unique_customers,
    sum(quantity)                  as total_items,
    round(sum(gross_amount), 2)    as gross_revenue,
    round(sum(discount_amount), 2) as total_discounts,
    round(sum(total_amount), 2)    as net_revenue,
    round(avg(total_amount), 2)    as avg_order_value

from orders
group by channel, order_date
order by order_date desc, net_revenue desc