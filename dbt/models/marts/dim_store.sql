-- Gold: store dimension, SCD Type 1 (current state only).
--
-- Surrogate key = a HASH of the business key (md5) instead of an IDENTITY number.
-- dbt rebuilds the table on every run; a hash gives the same key every time,
-- so facts never point to the wrong store after a rebuild.
with stores as (
    select store_code, store_name, store_type, city, district, open_date
    from {{ ref('stg_stores') }}

    union all
    -- the web shop (MongoDB orders) is not in the SQL Server store list: add it
    select 'WEB', 'Lusitânia Webshop', 'Web', 'Lisboa', 'Lisboa', date '2025-01-01'
)

select
    md5(store_code) as store_key,
    store_code,
    store_name,
    store_type,
    city,
    district,
    open_date
from stores

union all
-- Unknown member: facts with a missing store point here
select '-1', 'UNKNOWN', 'Unknown Store', 'Unknown', 'Unknown', 'Unknown', null
