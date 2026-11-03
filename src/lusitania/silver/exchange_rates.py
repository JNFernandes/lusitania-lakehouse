"""
silver/exchange_rates  ←  bronze/api/exchange_rates

Bronze holds ONE row: the whole API response as JSON text:
    {"base": "EUR", "rates": {"2025-01-02": {"GBP": 0.83, "USD": 1.03}, ...}}

Silver turns it into ONE ROW PER DAY AND CURRENCY:
    rate_date   currency  rate     is_published
    2025-01-02  GBP       0.8300   true
    2025-01-04  GBP       0.8300   false   ← Saturday: no ECB rate, previous day's used

Why fill the gaps: the ECB publishes rates on working days only, but customers
also buy at weekends and on holidays. Every calendar day must have a rate.

Meaning of "rate": units of that currency for 1 EUR (GBP 0.83 → €1 = £0.83),
so amount_eur = amount_in_currency / rate.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lusitania.silver.framework import is_unique, merge_into_silver, no_nulls, none_where, read_latest_batch, run_checks
from lusitania.storage import Lakehouse

TABLE = "exchange_rates"
KEYS = ["rate_date", "currency"]
BRONZE = "api/exchange_rates"

API_SCHEMA = T.StructType([
    T.StructField("base", T.StringType()),
    T.StructField("rates", T.MapType(T.StringType(), T.MapType(T.StringType(), T.DoubleType()))),
])


def clean(bronze: DataFrame) -> DataFrame:
    # 1. JSON text → one row per published day and currency
    parsed = bronze.select(F.from_json("raw_json", API_SCHEMA).alias("j"), "_batch_id")
    published = (parsed
        .select("j.base", "_batch_id", F.explode("j.rates").alias("day", "by_currency"))
        .select("base", "_batch_id", F.to_date("day").alias("rate_date"),
                F.explode("by_currency").alias("currency", "rate")))

    # 2. A full calendar for each currency: from 1 January of the first published year
    #    until today (not the last published day: an order placed on Saturday needs
    #    a rate even if the API was called before Monday's rate exists)
    bounds = published.agg(F.min("rate_date").alias("first"), F.max("rate_date").alias("last"))
    days = bounds.select(F.explode(F.sequence(
        F.trunc("first", "year"), F.greatest("last", F.current_date()))).alias("rate_date"))
    grid = days.crossJoin(published.select("currency", "base", "_batch_id").distinct())

    # 3. Fill the gaps: weekends and holidays take the last published rate
    #    (and days before the first published rate take the first one)
    filled = grid.join(published.select("rate_date", "currency", "rate"), ["rate_date", "currency"], "left")
    by_day = Window.partitionBy("currency").orderBy("rate_date")
    previous = F.last("rate", ignorenulls=True).over(by_day.rowsBetween(Window.unboundedPreceding, 0))
    following = F.first("rate", ignorenulls=True).over(by_day.rowsBetween(0, Window.unboundedFollowing))

    return filled.select(
        "rate_date",
        F.col("base").alias("base_currency"),
        "currency",
        F.coalesce(previous, following).cast("decimal(12,6)").alias("rate"),
        F.col("rate").isNotNull().alias("is_published"),
        F.col("_batch_id").alias("bronze_batch_id"),
        F.current_timestamp().alias("silver_updated_at"),
    )


def build(lake: Lakehouse) -> None:
    bronze = read_latest_batch(lake, BRONZE)
    merge_into_silver(lake, clean(bronze), TABLE, KEYS)

    silver = lake.read("silver", TABLE)
    days = silver.select("rate_date").distinct().count()
    currencies = silver.select("currency").distinct().count()
    run_checks(TABLE, {
        "rate_date + currency is unique": is_unique(silver, KEYS),
        "every day has a rate": no_nulls(silver, "rate"),
        "no gaps: rows = days x currencies": silver.count() == days * currencies,
        "rates are plausible (0.5 to 2.0)": none_where(silver, "rate < 0.5 OR rate > 2.0"),
    })
