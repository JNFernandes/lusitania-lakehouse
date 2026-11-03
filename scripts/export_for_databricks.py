"""
Exports the LATEST batch of every bronze table to one Parquet file each,
to upload to the Databricks volume.

    data/bronze/<source>/<table>   (Delta)   →   data/export/<source>__<table>.parquet

Databricks can't reach the SQL Server and MongoDB on this laptop, so the raw
data travels as files, like a source system dropping daily extracts.

Uses DuckDB (fast, no Spark needed).   Run:   python scripts/export_for_databricks.py
"""
from pathlib import Path

import duckdb

from lusitania import config


def main():
    config.EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    tables = sorted(p.parent for p in config.BRONZE_DIR.glob("*/*/_delta_log"))
    if not tables:
        raise SystemExit(f"No bronze tables in {config.BRONZE_DIR}. Run:  lusitania bronze")

    con = duckdb.connect()
    con.cursor().execute("INSTALL delta")
    con.cursor().execute("LOAD delta")

    print(f"Exporting {len(tables)} bronze tables to {config.EXPORT_DIR}\n")
    for table in tables:
        name = f"{table.parent.name}__{table.name}"             # e.g. sqlserver__sales_customer
        source = table.as_posix()
        target = (config.EXPORT_DIR / f"{name}.parquet").as_posix()
        con.cursor().execute(f"""
            COPY (
                SELECT * FROM delta_scan('{source}')
                WHERE _batch_id = (SELECT MAX(_batch_id) FROM delta_scan('{source}'))
            ) TO '{target}' (FORMAT parquet)
        """)
        rows = con.cursor().execute(f"SELECT COUNT(*) FROM read_parquet('{target}')").fetchone()[0]
        size_kb = Path(target).stat().st_size / 1024
        print(f"  {name + '.parquet':<44} {rows:>8,} rows  {size_kb:>8,.0f} KB")

    con.close()
    print(f"\nDone. Upload every file in {config.EXPORT_DIR} to your Databricks volume.")


if __name__ == "__main__":
    main()
