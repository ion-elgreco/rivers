select id, customer_id, cast(order_date as date) as order_date, amount from {{ ref('raw_orders') }}
