-- models/staging/stg_orders.sql
-- Cleans and standardises raw orders from Kafka ingestion

with source as (
    select * from {{ source('ecommerce', 'raw_orders') }}
),

staged as (
    select
        order_id,
        customer_id,
        customer_email,
        product_id,
        product_name,
        category,
        quantity,
        unit_price,
        discount_pct,
        total_amount,
        currency,
        lower(status)  as status,
        lower(channel) as channel,
        upper(country) as country,
        timestamp(created_at)  as created_at,
        timestamp(ingested_at) as ingested_at,

        -- derived fields
        case
            when status = 'completed' then true
            else false
        end as is_completed,

        round(unit_price * quantity, 2)          as gross_amount,
        round(unit_price * quantity * discount_pct / 100, 2) as discount_amount

    from source
    where order_id is not null
      and customer_id is not null
      and total_amount > 0
)

select * from staged