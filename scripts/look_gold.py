"""
Look inside the gold layer (the DuckDB file built by dbt).

    python scripts/look_gold.py           overview of every gold table
    python scripts/look_gold.py C00040    + the history of one customer: its snapshot rows
                                            and its SCD2 versions in dim_customer

Read-only: it never changes the database. Close it before running `dbt build`
(DuckDB allows only one writer at a time).
"""
import os
import sys

import duckdb

from lusitania import config

DB = config.GOLD_DATABASE


def query(con, sql: str, params=None) -> tuple[list, list]:
    """Run a query and return (column names, rows)."""
    cursor = con.cursor()
    cursor.execute(sql, params or [])
    columns = [c[0] for c in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    return columns, rows


def show(con, title: str, sql: str, params=None) -> None:
    """Print a query result as a simple text table."""
    columns, rows = query(con, sql, params)
    text = [[("" if v is None else str(v)) for v in row] for row in rows]
    widths = [max([len(c)] + [len(r[i]) for r in text]) for i, c in enumerate(columns)]
    line = "  ".join("-" * w for w in widths)
    print(f"\n=== {title} ===")
    print("  ".join(c.ljust(w) for c, w in zip(columns, widths, strict=True)))
    print(line)
    for r in text:
        print("  ".join(v.ljust(w) for v, w in zip(r, widths, strict=True)))
    if not rows:
        print("(no rows)")


def main():
    if not DB.exists():
        sys.exit(f"{DB} not found. Run first:  lusitania gold")

    # The staging views read silver with paths relative to the dbt folder
    # ('../data/silver/...'), so work from there, as dbt does.
    os.chdir(config.DBT_DIR)

    con = duckdb.connect(str(DB), read_only=True)
    try:
        con.cursor().execute("LOAD delta")      # the staging views read the silver Delta tables
    except duckdb.Error:
        pass

    # 1. Every table and view dbt built, with its row count
    _, objects = query(con, """
        SELECT table_schema, table_name, table_type
        FROM information_schema.tables
        WHERE table_schema IN ('staging', 'snapshots', 'gold')
        ORDER BY table_schema, table_name
    """)
    print("=== Objects in the gold database ===")
    for schema, name, kind in objects:
        _, result = query(con, f'SELECT COUNT(*) FROM "{schema}"."{name}"')
        kind = "view" if kind == "VIEW" else "table"
        print(f"  {schema + '.' + name:<32} {kind:<6} {result[0][0]:>10,} rows")

    # 2. A few rows of each dimension
    show(con, "gold.dim_store",
         "SELECT store_code, store_name, store_type, city FROM gold.dim_store ORDER BY store_code")

    show(con, "gold.dim_date (first week of October 2026)", """
        SELECT date_key, full_date, day_name, is_weekend, quarter_name, is_holiday, holiday_name
        FROM gold.dim_date
        WHERE full_date BETWEEN DATE '2026-10-01' AND DATE '2026-10-07'
        ORDER BY full_date
    """)

    show(con, "The product snowflake: product -> category -> department", """
        SELECT p.sku, p.product_name, p.list_price, c.category_name, d.department_name
        FROM gold.dim_product    p
        JOIN gold.dim_category   c ON c.category_key   = p.category_key
        JOIN gold.dim_department d ON d.department_key = c.department_key
        WHERE p.is_current AND p.sku <> 'UNKNOWN'
        ORDER BY p.sku
        LIMIT 5
    """)

    # 3. SCD2 overview: how many customers / products have more than one version
    show(con, "SCD2 versions", """
        SELECT 'customers' AS dimension,
               COUNT(DISTINCT customer_code)              AS members,
               COUNT(*)                                   AS rows_incl_history,
               COUNT(*) FILTER (WHERE version_number > 1) AS extra_versions
        FROM gold.dim_customer WHERE customer_code <> 'UNKNOWN'
        UNION ALL
        SELECT 'products', COUNT(DISTINCT sku), COUNT(*), COUNT(*) FILTER (WHERE version_number > 1)
        FROM gold.dim_product WHERE sku <> 'UNKNOWN'
    """)

    # 4. Optional: the full history of one customer
    if len(sys.argv) > 1:
        code = sys.argv[1].upper()
        show(con, f"Snapshot rows of customer {code} (what dbt snapshot stored)", """
            SELECT customer_code, city, district, segment, dbt_valid_from, dbt_valid_to
            FROM snapshots.snap_customers
            WHERE customer_code = ?
            ORDER BY dbt_valid_from
        """, [code])
        show(con, f"SCD2 versions of customer {code} in gold.dim_customer", """
            SELECT left(customer_key, 10) AS customer_key, customer_code, full_name, city, segment,
                   version_number, valid_from, valid_to, is_current
            FROM gold.dim_customer
            WHERE customer_code = ?
            ORDER BY version_number
        """, [code])

    con.close()


if __name__ == "__main__":
    main()
