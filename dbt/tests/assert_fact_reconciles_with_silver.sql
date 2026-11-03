-- RECONCILIATION (the most important check of Project 1, now automatic):
-- gold must contain exactly the lines and the euros that silver has.
-- Returns a row only if something was lost or duplicated on the way.
with source as (
    select count(*) as lines, sum(net_amount_eur) as net
    from (
        select net_amount_eur from {{ ref('stg_store_sales') }}
        union all
        select net_amount_eur from {{ ref('stg_online_sales') }}
    ) s
),
gold as (
    select count(*) as lines, sum(net_amount_eur) as net
    from {{ ref('fact_sales') }}
)
select source.lines as source_lines, gold.lines as gold_lines,
       source.net   as source_net,   gold.net   as gold_net
from source, gold
where source.lines <> gold.lines
   or source.net   <> gold.net
