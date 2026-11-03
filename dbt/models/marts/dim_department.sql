-- Gold: department dimension (top of the snowflake: product → category → department).
select distinct
    md5(cast(department_id as string)) as department_key,
    department_id,
    department_name
from {{ ref('stg_products') }}

union all
select '-1', -1, 'Unknown'
