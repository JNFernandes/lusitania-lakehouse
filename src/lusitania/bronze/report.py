"""
A quick look inside bronze: every table, its rows, batches and Delta versions,
plus one example of a raw MongoDB document. A report for a person, so it prints.
"""
from __future__ import annotations

from lusitania.storage import Lakehouse


def inspect_bronze(lake: Lakehouse) -> None:
    tables = lake.tables("bronze")
    if not tables:
        print("No bronze tables found. Run:  lusitania bronze")
        return

    print(f"{'table':<32}{'rows':>10}{'batches':>9}{'versions':>10}")
    for table in tables:
        df = lake.read("bronze", table)
        batches = df.select("_batch_id").distinct().count()
        versions = lake.history("bronze", table).count()
        print(f"{table:<32}{df.count():>10,}{batches:>9}{versions:>10}")

    if lake.exists("bronze", "mongo/orders"):
        row = lake.read("bronze", "mongo/orders").select("raw_json").limit(1).collect()[0]
        print("\nExample raw MongoDB document stored in bronze:")
        print(row.raw_json[:600] + " ...")
