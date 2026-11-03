-- Every product has exactly one current version.
select sku, count(*) as current_versions
from {{ ref('dim_product') }}
where is_current
group by sku
having count(*) <> 1
