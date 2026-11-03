"""
silver/stores  ←  bronze/sqlserver/sales_store

The simplest silver table: rename, trim, standard case.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lusitania.silver.framework import is_unique, merge_into_silver, no_nulls, read_latest_batch, run_checks
from lusitania.storage import Lakehouse

TABLE = "stores"
KEYS = ["store_code"]
BRONZE = "sqlserver/sales_store"


def clean(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.upper(F.trim("StoreCode")).alias("store_code"),
        F.trim("StoreName").alias("store_name"),
        F.trim("StoreType").alias("store_type"),
        F.trim("City").alias("city"),
        F.trim("District").alias("district"),
        F.col("OpenDate").alias("open_date"),
        F.col("ModifiedDate").alias("source_modified_at"),
        F.col("_batch_id").alias("bronze_batch_id"),
        F.current_timestamp().alias("silver_updated_at"),
    )


def build(lake: Lakehouse) -> None:
    bronze = read_latest_batch(lake, BRONZE)
    merge_into_silver(lake, clean(bronze), TABLE, KEYS)

    silver = lake.read("silver", TABLE)
    run_checks(TABLE, {
        "row count equals bronze latest batch": silver.count() == bronze.count(),
        "store_code is unique": is_unique(silver, KEYS),
        "no missing code, name or city": no_nulls(silver, "store_code", "store_name", "city"),
    })
