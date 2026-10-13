select order_id, cast(amount as integer) as amount from {{ source('raw', 'payments') }}
