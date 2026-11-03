{#
  SNAPSHOT = SCD Type 2, done by dbt.
  Each `dbt snapshot` run compares stg_customers with what was stored last time.
  If city, district or segment changed for a customer, dbt closes the old row
  (fills dbt_valid_to) and inserts a new one. Other columns are NOT tracked:
  dim_customer takes them from the current data (Type 1), like Project 1.
#}
{% snapshot snap_customers %}
{{ config(
    schema='snapshots',
    unique_key='customer_code',
    strategy='check',
    check_cols=['city', 'district', 'segment']
) }}

select customer_code, city, district, segment
from {{ ref('stg_customers') }}

{% endsnapshot %}
