-- Staging: web order lines from MongoDB, already converted to EUR in silver.
select
    order_number,
    line_number,
    order_ts,
    order_date,
    channel,                              -- 'Web'
    status,                               -- paid / shipped / delivered / cancelled / returned
    payment_method,
    'WEB'                 as store_code,  -- web orders get their own "store" in dim_store
    customer_code,                        -- NULL for guest checkouts
    sku,
    quantity,
    currency,                             -- currency the customer paid in
    unit_price_eur,
    cast(0 as decimal(12,2)) as discount_amount_eur,
    net_amount_eur,
    source_system
from {{ source('silver', 'online_sales_lines') }}
