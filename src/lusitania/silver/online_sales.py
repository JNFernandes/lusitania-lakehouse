"""
silver/online_sales_lines  ←  bronze/mongo/orders  +  silver/exchange_rates

Where the raw, messy JSON becomes clean rows. Six fixes, in order:

  1. PARSE        raw JSON text → typed columns, with an explicit schema
  2. DATES        three formats in "orderDate" → one timestamp
  3. DEDUPLICATE  the same orderNumber stored twice → keep the latest version
  4. EXPLODE      the nested "items" array → one row per item (grain = order line)
  5. STANDARDISE  lowercase SKUs, missing payment, guest checkouts
  6. CONVERT      GBP / USD prices → EUR, with the rate of the ORDER DATE

Grain: ONE ROW PER ORDER LINE, same as silver/store_sales_lines, so gold can
put both channels in one fact table.
"""
from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from lusitania.silver.framework import is_unique, merge_into_silver, no_nulls, none_where, read_latest_batch, run_checks
from lusitania.storage import Lakehouse

TABLE = "online_sales_lines"
KEYS = ["order_number", "line_number"]
BRONZE = "mongo/orders"

# 1. The shape we EXPECT the documents to have. Fields missing in a document
#    simply become NULL, which is how guest orders and missing payments show up.
#    orderDate is read as text on purpose, because it comes in several formats.
ORDER_SCHEMA = T.StructType([
    T.StructField("orderNumber", T.StringType()),
    T.StructField("orderDate", T.StringType()),
    T.StructField("status", T.StringType()),
    T.StructField("currency", T.StringType()),
    T.StructField("guestEmail", T.StringType()),
    T.StructField("createdAt", T.StructType([T.StructField("$date", T.StringType())])),
    T.StructField("customer", T.StructType([
        T.StructField("code", T.StringType()),
        T.StructField("email", T.StringType()),
        T.StructField("country", T.StringType()),
    ])),
    T.StructField("shipping", T.StructType([
        T.StructField("city", T.StringType()),
        T.StructField("country", T.StringType()),
        T.StructField("cost", T.DoubleType()),
    ])),
    T.StructField("payment", T.StructType([T.StructField("method", T.StringType())])),
    T.StructField("items", T.ArrayType(T.StructType([
        T.StructField("line", T.IntegerType()),
        T.StructField("sku", T.StringType()),
        T.StructField("name", T.StringType()),
        T.StructField("qty", T.IntegerType()),
        T.StructField("unitPrice", T.DoubleType()),
    ]))),
])

DATE_FORMATS = [
    "yyyy-MM-dd'T'HH:mm:ssX",        # 2025-06-10T14:32:00Z
    "yyyy-MM-dd'T'HH:mm:ss.SSSX",    # 2025-06-10T14:32:00.000Z
    "dd/MM/yyyy HH:mm",              # 10/06/2025 14:32
]


def parse_order_date(col: Column) -> Column:
    """2. The three formats seen in the source → one timestamp (UTC); NULL if none fits."""
    # a real BSON date arrives as the text {"$date": "2025-06-10T14:32:00Z"}: take the inside
    inner = F.regexp_extract(col, r'"\$date"\s*:\s*"([^"]+)"', 1)
    text = F.when(F.trim(col).startswith("{"), inner).otherwise(F.trim(col))
    # try_to_timestamp returns NULL instead of failing (Databricks runs in ANSI mode)
    return F.coalesce(*[F.try_to_timestamp(text, F.lit(fmt)) for fmt in DATE_FORMATS])


def clean(bronze: DataFrame, rates: DataFrame) -> DataFrame:
    # 1. PARSE
    orders = (bronze.select(F.from_json("raw_json", ORDER_SCHEMA).alias("o"), "_batch_id")
                    .select("o.*", "_batch_id"))

    # 2. DATES
    orders = (orders
        .withColumn("order_ts", parse_order_date(F.col("orderDate")))
        .withColumn("created_ts", F.to_timestamp(F.col("createdAt.`$date`"))))

    # 3. DEDUPLICATE: keep the most recently stored version of each order
    latest_first = Window.partitionBy("orderNumber").orderBy(F.col("created_ts").desc())
    orders = orders.withColumn("_rn", F.row_number().over(latest_first)).filter("_rn = 1").drop("_rn")

    # 4. EXPLODE: one row per item; the order's fields are repeated on each row
    lines = orders.select("*", F.explode("items").alias("item")).drop("items")

    # 5. STANDARDISE
    lines = lines.select(
        F.upper(F.trim("orderNumber")).alias("order_number"),
        F.col("item.line").alias("line_number"),
        "order_ts",
        F.to_date("order_ts").alias("order_date"),
        F.lit("Web").alias("channel"),
        F.lower(F.trim("status")).alias("status"),
        F.coalesce(F.lower(F.trim("payment.method")), F.lit("unknown")).alias("payment_method"),
        F.upper(F.trim("customer.code")).alias("customer_code"),          # NULL for guests
        F.col("customer").isNull().alias("is_guest"),
        F.lower(F.coalesce("customer.email", "guestEmail")).alias("customer_email"),
        F.col("shipping.city").alias("ship_city"),
        F.upper("shipping.country").alias("ship_country"),
        F.upper(F.trim("item.sku")).alias("sku"),                         # sku-00010 → SKU-00010
        F.trim("item.name").alias("product_name"),
        F.col("item.qty").alias("quantity"),
        F.upper(F.trim("currency")).alias("currency"),
        F.col("item.unitPrice").cast("decimal(10,2)").alias("unit_price_local"),
        F.col("created_ts").alias("source_modified_at"),
        F.col("_batch_id").alias("bronze_batch_id"),
    )

    # 6. CONVERT TO EUR: join the rate of the order's own date and currency (EUR = 1)
    r = rates.select(F.col("rate_date").alias("order_date"), "currency", F.col("rate").alias("fx_rate"))
    lines = (lines.join(r, ["order_date", "currency"], "left")
                  .withColumn("fx_rate", F.when(F.col("currency") == "EUR", F.lit(1))
                                          .otherwise(F.col("fx_rate")).cast("decimal(12,6)")))

    unit_eur = F.round(F.col("unit_price_local") / F.col("fx_rate"), 2)
    return lines.select(
        *KEYS, "order_ts", "order_date", "channel", "status", "payment_method",
        "customer_code", "is_guest", "customer_email", "ship_city", "ship_country",
        "sku", "product_name", "quantity", "currency", "unit_price_local", "fx_rate",
        unit_eur.cast("decimal(10,2)").alias("unit_price_eur"),
        F.round(unit_eur * F.col("quantity"), 2).cast("decimal(12,2)").alias("net_amount_eur"),
        F.lit("mongodb").alias("source_system"),
        "source_modified_at", "bronze_batch_id",
        F.current_timestamp().alias("silver_updated_at"),
    )


def build(lake: Lakehouse) -> None:
    bronze = read_latest_batch(lake, BRONZE)
    rates = lake.read("silver", "exchange_rates")
    merge_into_silver(lake, clean(bronze, rates), TABLE, KEYS)

    silver = lake.read("silver", TABLE)
    known_skus = lake.read("silver", "products").select("sku")
    unknown_skus = silver.select("sku").distinct().join(known_skus, "sku", "left_anti").count()
    run_checks(TABLE, {
        "order_number + line_number is unique": is_unique(silver, KEYS),
        "every order date was parsed": no_nulls(silver, "order_ts"),
        "every line has an EUR price": no_nulls(silver, "fx_rate", "unit_price_eur"),
        "every SKU exists in silver/products": unknown_skus == 0,
        "SKUs follow the SKU-00000 pattern": none_where(silver, r"NOT sku RLIKE '^SKU-[0-9]{5}$'"),
        "quantity is positive": none_where(silver, "quantity <= 0"),
    })
