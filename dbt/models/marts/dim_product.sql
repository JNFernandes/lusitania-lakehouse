-- Gold: product dimension, SCD Type 2 (bottom of the snowflake).
-- Holds category_key (→ dim_category → dim_department) instead of the
-- category and department names: that is the difference from a star schema.
with versions as (
    select
        sku,
        category_id,
        list_price,
        dbt_valid_from,
        dbt_valid_to,
        row_number() over (partition by sku order by dbt_valid_from) as version_number
    from {{ ref('snap_products') }}
),

current_attributes as (
    select sku, product_name, brand, is_active
    from {{ ref('stg_products') }}
)

select
    md5(v.sku || '|' || cast(v.dbt_valid_from as string))  as product_key,
    v.sku,
    c.product_name,                                         -- Type 1
    c.brand,                                                -- Type 1
    c.is_active,                                            -- Type 1
    md5(cast(v.category_id as string))                     as category_key,   -- SCD2, snowflake link
    v.list_price,                                           -- SCD2
    v.version_number,
    case when v.version_number = 1 then timestamp '1900-01-01' else v.dbt_valid_from end as valid_from,
    coalesce(v.dbt_valid_to, timestamp '9999-12-31')        as valid_to,
    v.dbt_valid_to is null                                  as is_current
from versions v
left join current_attributes c on c.sku = v.sku

union all
select '-1', 'UNKNOWN', 'Unknown Product', 'Unknown', false, '-1',
       cast(0 as decimal(10,2)), 1, timestamp '1900-01-01', timestamp '9999-12-31', true
