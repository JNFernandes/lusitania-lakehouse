"""
SILVER: clean, deduplicated, typed tables with business keys, one module per table.

Every table module has the same shape:

    TABLE, KEYS        the silver table name and its business key
    clean(...)         PURE: bronze DataFrame(s) in → clean DataFrame out (no reading, no writing)
    build(lake)        READ the latest bronze batch → clean() → MERGE into silver → CHECK

Keeping clean() pure is what makes it unit-testable: a test builds a tiny
DataFrame by hand, calls clean(), and checks the result. No SQL Server,
MongoDB or Delta files needed.
"""
