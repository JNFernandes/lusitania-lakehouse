-- Staging: silver products, already joined to category and department in silver.
select
    sku,
    product_name,
    brand,
    category_id,
    category_name,
    department_id,
    department_name,
    list_price,
    is_active
from {{ source('silver', 'products') }}
