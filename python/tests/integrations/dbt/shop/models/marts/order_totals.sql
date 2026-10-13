{{ config(materialized='ephemeral') }}

select o.customer_id, sum(p.amount) as paid
from {{ ref('stg_orders') }} o
join {{ ref('stg_payments') }} p on p.order_id = o.id
group by 1
