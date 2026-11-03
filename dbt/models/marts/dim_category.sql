-- Gold: category dimension (middle of the snowflake).
-- Points UP to its department with department_key, instead of repeating the department name.
select distinct
    md5(cast(category_id as string))   as category_key,
    category_id,
    category_name,
    md5(cast(department_id as string)) as department_key
from {{ ref('stg_products') }}

union all
select '-1', -1, 'Unknown', '-1'
