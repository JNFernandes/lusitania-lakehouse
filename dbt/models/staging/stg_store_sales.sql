-- Staging: in-store sales lines from SQL Server, shaped like the web lines
-- so both channels can be combined in fact_sales.
select
    order_number,
    line_number,
    order_ts,
    order_date,
    channel,                              -- 'Store' or 'Online' (the old online shop in SQL Server)
    'completed'           as status,      -- the till system only records completed sales
    payment_method,
    store_code,
    customer_code,                        -- NULL for walk-ins
    sku,
    quantity,
    'EUR'                 as currency,
    unit_price_eur,
    discount_amount_eur,
    net_amount_eur,
    source_system
from {{ source('silver', 'store_sales_lines') }}
