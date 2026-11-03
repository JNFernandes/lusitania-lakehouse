-- Every sales line should find its product. A WARNING, not an error:
-- the line is kept (on the Unknown product) but someone should look at it.
{{ config(severity='warn') }}

select order_number, line_number, source_system
from {{ ref('fact_sales') }}
where product_key = '-1'
