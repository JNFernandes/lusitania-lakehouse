{#
  SCD Type 2 for products: a new version when the product moves to another
  category or its list price changes. The category's own NAME is not tracked
  here: in the snowflake design it lives in dim_category, so renaming a
  category changes one row there instead of a version of every product.
#}
{% snapshot snap_products %}
{{ config(
    schema='snapshots',
    unique_key='sku',
    strategy='check',
    check_cols=['category_id', 'list_price']
) }}

select sku, category_id, list_price
from {{ ref('stg_products') }}

{% endsnapshot %}
