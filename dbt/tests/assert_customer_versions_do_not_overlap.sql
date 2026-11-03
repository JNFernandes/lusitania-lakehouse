-- Each version must end exactly when the next one starts: no gaps, no overlaps.
-- Otherwise a sale could match two versions of the same customer, or none.
with ordered as (
    select
        customer_code,
        valid_to,
        lead(valid_from) over (partition by customer_code order by valid_from) as next_valid_from
    from {{ ref('dim_customer') }}
)
select *
from ordered
where next_valid_from is not null
  and next_valid_from <> valid_to
