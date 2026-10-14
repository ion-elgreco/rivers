{{ config(severity='warn') }}

select * from {{ ref('stg_orders') }} where amount > 5
