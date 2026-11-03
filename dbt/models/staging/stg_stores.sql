-- Staging: silver stores.
select
    store_code,
    store_name,
    store_type,
    city,
    district,
    open_date
from {{ source('silver', 'stores') }}
