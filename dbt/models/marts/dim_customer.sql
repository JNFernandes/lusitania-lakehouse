-- Gold: customer dimension, SCD Type 2 (from the snapshot) + Type 1 attributes.
--
--   one row per VERSION of a customer
--   customer_key   = md5(customer_code | valid_from): a different key per version
--   valid_from     = the first version starts in 1900, so every past sale finds it
--   valid_to       = 9999-12-31 for the current version
--   names, email, phone: always today's values (Type 1), taken from stg_customers
with versions as (
    select
        customer_code,
        city,
        district,
        segment,
        dbt_valid_from,
        dbt_valid_to,
        row_number() over (partition by customer_code order by dbt_valid_from) as version_number
    from {{ ref('snap_customers') }}
),

current_attributes as (
    select customer_code, first_name, last_name, full_name, email, phone, birth_date, created_date
    from {{ ref('stg_customers') }}
)

select
    md5(v.customer_code || '|' || cast(v.dbt_valid_from as string))          as customer_key,
    v.customer_code,
    c.first_name,
    c.last_name,
    c.full_name,
    c.email,
    c.phone,
    c.birth_date,
    c.created_date,
    v.city,                                                                   -- SCD2
    v.district,                                                               -- SCD2
    v.segment,                                                                -- SCD2
    v.version_number,
    case when v.version_number = 1 then timestamp '1900-01-01' else v.dbt_valid_from end as valid_from,
    coalesce(v.dbt_valid_to, timestamp '9999-12-31')                          as valid_to,
    v.dbt_valid_to is null                                                    as is_current
from versions v
left join current_attributes c on c.customer_code = v.customer_code

union all
-- Unknown member: walk-ins and guest checkouts point here
select '-1', 'UNKNOWN', 'Unknown', 'Customer', 'Unknown Customer', null, null, null, null,
       'Unknown', 'Unknown', 'Unknown', 1, timestamp '1900-01-01', timestamp '9999-12-31', true
