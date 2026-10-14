select id, name from {{ ref('raw_customers') }}
