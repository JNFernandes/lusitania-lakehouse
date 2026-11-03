"""
The steps every silver table shares, written once:

    read_latest_batch(lake, table)        1. READ   only the newest bronze batch
    (each table's own clean())            2. CLEAN
    merge_into_silver(lake, df, ...)      3. MERGE  idempotent upsert on the business key
    run_checks(table, {...})              4. CHECK  stop the pipeline if one fails
"""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lusitania.storage import Lakehouse

log = logging.getLogger(__name__)


class DataQualityError(Exception):
    """A silver check failed: the table must not be used to build gold."""


def latest_batch(bronze: DataFrame) -> DataFrame:
    """Only the rows of the newest batch (bronze keeps every batch ever loaded)."""
    newest = bronze.agg(F.max("_batch_id")).first()[0]
    return bronze.filter(F.col("_batch_id") == newest)


def read_latest_batch(lake: Lakehouse, table: str) -> DataFrame:
    return latest_batch(lake.read("bronze", table))


def merge_into_silver(lake: Lakehouse, df: DataFrame, table: str, keys: list[str]) -> None:
    """New key → insert. Existing key with a changed value → update. Unchanged → nothing."""
    if not lake.exists("silver", table):
        lake.write(df, "silver", table, mode="errorifexists")      # first run: create the table
        log.info("created %s with %s rows", lake.name("silver", table), f"{df.count():,}")
        return
    inserted, updated = lake.merge(df, "silver", table, keys)
    log.info("merged into %s: %s inserted, %s updated", lake.name("silver", table), inserted, updated)


def run_checks(table: str, checks: dict[str, bool]) -> None:
    """checks = {"description": passed}. Logs each one; raises DataQualityError if any failed."""
    for name, passed in checks.items():
        if passed:
            log.info("  [PASS] %s", name)
        else:
            log.error("  [FAIL] %s", name)
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise DataQualityError(f"silver/{table}: {len(failed)} check(s) failed: {failed}")


def is_unique(df: DataFrame, keys: list[str]) -> bool:
    return df.groupBy(*keys).count().filter("count > 1").limit(1).count() == 0


def no_nulls(df: DataFrame, *columns: str) -> bool:
    condition = " OR ".join(f"{c} IS NULL" for c in columns)
    return df.filter(condition).limit(1).count() == 0


def none_where(df: DataFrame, condition: str) -> bool:
    """True when no row matches the condition, e.g. none_where(df, "quantity <= 0")."""
    return df.filter(condition).limit(1).count() == 0
