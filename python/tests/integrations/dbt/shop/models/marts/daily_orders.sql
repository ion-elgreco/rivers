{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='order_date') }}

select order_date, count(*) as orders, sum(amount) as amount
from {{ ref('stg_orders') }}
where order_date = cast('{{ var("day", "2024-01-01") }}' as date)
group by 1
