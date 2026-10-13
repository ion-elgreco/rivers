select c.id, c.name, coalesce(t.paid, 0) as paid
from {{ ref('stg_customers') }} c
left join {{ ref('order_totals') }} t on t.customer_id = c.id
