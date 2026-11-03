-- Gold: sales fact table. GRAIN: one row per order line, BOTH channels.
--
--   1. stack the in-store lines (SQL Server) and the web lines (MongoDB):
--      the staging views give both exactly the same columns
--   2. swap business keys for SURROGATE KEYS from the dimensions
--      - customer and product: the version VALID AT THE ORDER TIME (point-in-time lookup)
--      - nothing found (walk-in, guest, unknown code) → '-1', the Unknown member
--   3. keep the measures in EUR
with lines as (
    select * from {{ ref('stg_store_sales') }}
    union all
    select * from {{ ref('stg_online_sales') }}
)

select
    md5(l.source_system || '|' || l.order_number || '|' || cast(l.line_number as string)) as sales_line_key,

    -- foreign keys to the dimensions
    coalesce(d.date_key, -1)        as date_key,
    coalesce(c.customer_key, '-1')  as customer_key,
    coalesce(p.product_key, '-1')   as product_key,
    coalesce(s.store_key, '-1')     as store_key,

    -- degenerate dimensions: attributes with no table of their own
    l.order_number,
    l.line_number,
    l.order_ts,
    l.channel,                      -- Store / Online / Web
    l.status,
    l.payment_method,
    l.currency                      as paid_currency,
    l.source_system,

    -- measures (EUR)
    l.quantity,
    l.unit_price_eur,                                                              -- non-additive
    cast(l.unit_price_eur * l.quantity as decimal(12,2))       as gross_amount_eur,
    l.discount_amount_eur,
    l.net_amount_eur,

    -- convenience flag: cancelled and returned web orders are not revenue
    l.status not in ('cancelled', 'returned')                  as is_revenue

from lines l

left join {{ ref('dim_date') }} d
       on d.full_date = l.order_date

left join {{ ref('dim_store') }} s
       on s.store_code = l.store_code

-- point-in-time lookups: the version whose validity period contains the order time
left join {{ ref('dim_customer') }} c
       on c.customer_code = l.customer_code
      and l.order_ts >= c.valid_from
      and l.order_ts <  c.valid_to

left join {{ ref('dim_product') }} p
       on p.sku = l.sku
      and l.order_ts >= p.valid_from
      and l.order_ts <  p.valid_to
