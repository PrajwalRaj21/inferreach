-- models/marts/fct_orders.sql
-- Final fact table for orders analytics

with orders as (
    select * from {{ ref('stg_orders') }}
)

select
    order_id,
    customer_id,
    customer_email,
    product_id,
    product_name,
    category,
    channel,
    country,
    quantity,
    unit_price,
    discount_pct,
    discount_amount,
    gross_amount,
    total_amount,
    currency,
    status,
    is_completed,
    created_at,
    ingested_at,

    -- time dimensions
    date(created_at)                        as order_date,
    format_timestamp('%Y-%m', created_at)   as order_month,
    extract(hour from created_at)           as order_hour,
    extract(dayofweek from created_at)      as day_of_week

from orders