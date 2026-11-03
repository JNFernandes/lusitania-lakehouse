"""
silver/customers  ←  bronze/sqlserver/sales_customer

One row per customer: trimmed text, standard case, snake_case names,
and one row per customer_code even if the source ever sends duplicates.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lusitania.silver.framework import is_unique, merge_into_silver, no_nulls, read_latest_batch, run_checks
from lusitania.storage import Lakehouse

TABLE = "customers"
KEYS = ["customer_code"]
BRONZE = "sqlserver/sales_customer"


def clean(bronze: DataFrame) -> DataFrame:
    df = bronze.select(
        F.upper(F.trim("CustomerCode")).alias("customer_code"),
        F.initcap(F.trim("FirstName")).alias("first_name"),
        F.initcap(F.trim("LastName")).alias("last_name"),
        F.lower(F.trim("Email")).alias("email"),
        F.trim("Phone").alias("phone"),
        F.trim("City").alias("city"),
        F.trim("District").alias("district"),
        F.trim("Segment").alias("segment"),
        F.col("BirthDate").alias("birth_date"),
        F.col("CreatedDate").alias("created_date"),
        F.col("ModifiedDate").alias("source_modified_at"),
        F.col("_batch_id").alias("bronze_batch_id"),
    )
    # One row per customer: if a code appears twice, keep the most recently modified row.
    # (The SQL Server source can't have duplicates, but silver must not trust that.)
    newest_first = Window.partitionBy(*KEYS).orderBy(F.col("source_modified_at").desc())
    df = df.withColumn("_rn", F.row_number().over(newest_first)).filter("_rn = 1").drop("_rn")
    return df.withColumn("silver_updated_at", F.current_timestamp())


def build(lake: Lakehouse) -> None:
    bronze = read_latest_batch(lake, BRONZE)
    merge_into_silver(lake, clean(bronze), TABLE, KEYS)

    silver = lake.read("silver", TABLE)
    run_checks(TABLE, {
        "one row per customer in the latest batch": silver.count() == bronze.select("CustomerCode").distinct().count(),
        "customer_code is unique": is_unique(silver, KEYS),
        "no missing code or email": no_nulls(silver, "customer_code", "email"),
    })
