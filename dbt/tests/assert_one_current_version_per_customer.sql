-- A SINGULAR test: a query that returns the rows that are WRONG.
-- dbt passes the test when it returns nothing.
-- Rule: every customer has exactly one current version (same rule as the
-- filtered unique index on dim.Customer in Project 1).
select customer_code, count(*) as current_versions
from {{ ref('dim_customer') }}
where is_current
group by customer_code
having count(*) <> 1
