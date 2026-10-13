{% snapshot customers_snapshot %}

{{ config(target_schema='snapshots', unique_key='id', strategy='check', check_cols=['name']) }}

select id, name from {{ ref('stg_customers') }}

{% endsnapshot %}
