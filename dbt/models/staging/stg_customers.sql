-- Staging: silver customers, with the columns gold needs. A VIEW: no data copied.
select
    customer_code,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    phone,
    city,
    district,
    segment,
    birth_date,
    created_date
from {{ source('silver', 'customers') }}
