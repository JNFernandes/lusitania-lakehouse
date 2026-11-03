# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — bronze Delta tables → clean silver tables
# MAGIC
# MAGIC The same six tables, cleaning and checks as `transformations/run_silver.py` on the
# MAGIC laptop, now reading and writing **Unity Catalog tables by name**:
# MAGIC
# MAGIC | silver table | built from (bronze) |
# MAGIC |---|---|
# MAGIC | `customers` | `sqlserver_sales_customer` |
# MAGIC | `products` | `sqlserver_product_product` + `_category` + `_department` |
# MAGIC | `stores` | `sqlserver_sales_store` |
# MAGIC | `exchange_rates` | `api_exchange_rates` |
# MAGIC | `store_sales_lines` | `sqlserver_sales_salesorder` + `_salesorderline` + store, customer, product |
# MAGIC | `online_sales_lines` | `mongo_orders` + silver `exchange_rates` |
# MAGIC
# MAGIC Every table follows the same four steps: **READ** the latest bronze batch →
# MAGIC **CLEAN** → **MERGE** into silver (idempotent) → **CHECK** (stop if a check fails).

# COMMAND ----------

dbutils.widgets.text("catalog", "lusitania")
CATALOG = dbutils.widgets.get("catalog")
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
print(f"{BRONZE}  →  {SILVER}")

# COMMAND ----------

# MAGIC %md ## Shared helpers (the same as `common/silver_utils.py` on the laptop)

# COMMAND ----------

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

AUDIT_COLUMNS = ("bronze_batch_id", "silver_updated_at")


def read_latest_batch(table: str) -> DataFrame:
    """READ: only the newest batch of a bronze table."""
    df = spark.table(f"{BRONZE}.{table}")
    latest = df.agg(F.max("_batch_id")).first()[0]
    return df.filter(F.col("_batch_id") == latest)


def merge_into_silver(df: DataFrame, table: str, keys: list) -> None:
    """MERGE: new key → insert, changed row → update, unchanged → nothing."""
    target = f"{SILVER}.{table}"
    if not spark.catalog.tableExists(target):
        df.write.format("delta").saveAsTable(target)
        print(f"  created {target} with {spark.table(target).count():,} rows")
        return

    df.createOrReplaceTempView("source_rows")
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    compare = [c for c in df.columns if c not in keys and c not in AUDIT_COLUMNS]
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in compare)   # <=> = null-safe equals

    metrics = spark.sql(f"""
        MERGE INTO {target} AS t
        USING source_rows AS s
        ON {on}
        WHEN MATCHED AND ({changed}) THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """).first()
    print(f"  merged into {target}: {metrics['num_inserted_rows']} inserted, "
          f"{metrics['num_updated_rows']} updated")


def run_checks(table: str, checks: dict) -> None:
    """CHECK: print each result; stop the notebook if any check failed."""
    print(f"  checks for {table}:")
    for name, passed in checks.items():
        print(f"    [{'PASS' if passed else 'FAIL'}] {name}")
    failed = [n for n, ok in checks.items() if not ok]
    if failed:
        raise Exception(f"{table}: {len(failed)} check(s) failed: {failed}")


def is_unique(df: DataFrame, keys: list) -> bool:
    return df.groupBy(*keys).count().filter("count > 1").limit(1).count() == 0


def no_nulls(df: DataFrame, *cols: str) -> bool:
    return df.filter(" OR ".join(f"{c} IS NULL" for c in cols)).limit(1).count() == 0

# COMMAND ----------

# MAGIC %md ## 1. Customers

# COMMAND ----------

bronze = read_latest_batch("sqlserver_sales_customer")

customers = bronze.select(
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
newest_first = Window.partitionBy("customer_code").orderBy(F.col("source_modified_at").desc())
customers = (customers.withColumn("_rn", F.row_number().over(newest_first))
                      .filter("_rn = 1").drop("_rn")
                      .withColumn("silver_updated_at", F.current_timestamp()))

merge_into_silver(customers, "customers", ["customer_code"])
silver = spark.table(f"{SILVER}.customers")
run_checks("customers", {
    "row count equals bronze latest batch": silver.count() == bronze.count(),
    "customer_code is unique": is_unique(silver, ["customer_code"]),
    "no missing code or email": no_nulls(silver, "customer_code", "email"),
})

# COMMAND ----------

# MAGIC %md ## 2. Products (joined to category and department)

# COMMAND ----------

p = read_latest_batch("sqlserver_product_product")
c = read_latest_batch("sqlserver_product_category")
d = read_latest_batch("sqlserver_product_department")

products = (
    p.select(F.upper(F.trim("SKU")).alias("sku"), F.trim("ProductName").alias("product_name"),
             F.trim("Brand").alias("brand"), F.col("CategoryID").alias("category_id"),
             F.col("ListPrice").cast("decimal(10,2)").alias("list_price"),
             F.col("IsActive").alias("is_active"), F.col("ModifiedDate").alias("p_mod"),
             F.col("_batch_id").alias("bronze_batch_id"))
    .join(c.select(F.col("CategoryID").alias("category_id"), F.trim("CategoryName").alias("category_name"),
                   F.col("DepartmentID").alias("department_id"), F.col("ModifiedDate").alias("c_mod")),
          "category_id", "left")
    .join(d.select(F.col("DepartmentID").alias("department_id"), F.trim("DepartmentName").alias("department_name"),
                   F.col("ModifiedDate").alias("d_mod")),
          "department_id", "left")
    .select("sku", "product_name", "brand", "category_id", "category_name", "department_id",
            "department_name", "list_price", "is_active",
            F.greatest("p_mod", "c_mod", "d_mod").alias("source_modified_at"),
            "bronze_batch_id", F.current_timestamp().alias("silver_updated_at"))
)

merge_into_silver(products, "products", ["sku"])
silver = spark.table(f"{SILVER}.products")
run_checks("products", {
    "row count equals bronze latest batch": silver.count() == p.count(),
    "sku is unique": is_unique(silver, ["sku"]),
    "every product found its category and department": no_nulls(silver, "category_name", "department_name"),
    "list_price is positive": silver.filter("list_price <= 0").limit(1).count() == 0,
})

# COMMAND ----------

# MAGIC %md ## 3. Stores

# COMMAND ----------

bronze = read_latest_batch("sqlserver_sales_store")
stores = bronze.select(
    F.upper(F.trim("StoreCode")).alias("store_code"), F.trim("StoreName").alias("store_name"),
    F.trim("StoreType").alias("store_type"), F.trim("City").alias("city"),
    F.trim("District").alias("district"), F.col("OpenDate").alias("open_date"),
    F.col("ModifiedDate").alias("source_modified_at"), F.col("_batch_id").alias("bronze_batch_id"),
    F.current_timestamp().alias("silver_updated_at"),
)
merge_into_silver(stores, "stores", ["store_code"])
silver = spark.table(f"{SILVER}.stores")
run_checks("stores", {
    "row count equals bronze latest batch": silver.count() == bronze.count(),
    "store_code is unique": is_unique(silver, ["store_code"]),
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Exchange rates
# MAGIC One API response (JSON text) → one row per day and currency; weekends and holidays
# MAGIC take the previous working day's rate.

# COMMAND ----------

API_SCHEMA = T.StructType([
    T.StructField("base", T.StringType()),
    T.StructField("rates", T.MapType(T.StringType(), T.MapType(T.StringType(), T.DoubleType()))),
])
bronze = read_latest_batch("api_exchange_rates")

published = (bronze.select(F.from_json("raw_json", API_SCHEMA).alias("j"), "_batch_id")
    .select("j.base", "_batch_id", F.explode("j.rates").alias("day", "by_currency"))
    .select("base", "_batch_id", F.to_date("day").alias("rate_date"),
            F.explode("by_currency").alias("currency", "rate")))

first_day = published.agg(F.min("rate_date")).first()[0]
last_day = published.agg(F.max("rate_date")).first()[0]
days = spark.range(1).select(F.explode(F.sequence(
    F.trunc(F.lit(first_day), "year"),                        # from 1 January of the first year
    F.greatest(F.lit(last_day), F.current_date()))).alias("rate_date"))
grid = days.crossJoin(published.select("currency", "base", "_batch_id").distinct())

by_day = Window.partitionBy("currency").orderBy("rate_date")
filled = grid.join(published.select("rate_date", "currency", "rate"), ["rate_date", "currency"], "left")
rates = filled.select(
    "rate_date", F.col("base").alias("base_currency"), "currency",
    F.coalesce(F.last("rate", True).over(by_day.rowsBetween(Window.unboundedPreceding, 0)),
               F.first("rate", True).over(by_day.rowsBetween(0, Window.unboundedFollowing)))
     .cast("decimal(12,6)").alias("rate"),
    F.col("rate").isNotNull().alias("is_published"),
    F.col("_batch_id").alias("bronze_batch_id"), F.current_timestamp().alias("silver_updated_at"),
)

merge_into_silver(rates, "exchange_rates", ["rate_date", "currency"])
silver = spark.table(f"{SILVER}.exchange_rates")
run_checks("exchange_rates", {
    "rate_date + currency is unique": is_unique(silver, ["rate_date", "currency"]),
    "every day has a rate": no_nulls(silver, "rate"),
    "rates are plausible (0.5 to 2.0)": silver.filter("rate < 0.5 OR rate > 2.0").limit(1).count() == 0,
})

# COMMAND ----------

# MAGIC %md ## 5. Store sales lines (SQL Server) — internal IDs replaced by business keys

# COMMAND ----------

o = read_latest_batch("sqlserver_sales_salesorder")
l = read_latest_batch("sqlserver_sales_salesorderline")
s = read_latest_batch("sqlserver_sales_store").select("StoreID", "StoreCode")
cu = read_latest_batch("sqlserver_sales_customer").select("CustomerID", "CustomerCode")
pr = read_latest_batch("sqlserver_product_product").select("ProductID", "SKU")

joined = (l.select("OrderID", "LineNumber", "ProductID", "Quantity", "UnitPrice", "DiscountPct",
                   F.col("ModifiedDate").alias("l_mod"))
          .join(o.select("OrderID", "OrderNumber", "OrderDate", "StoreID", "CustomerID", "Channel",
                         "PaymentMethod", F.col("ModifiedDate").alias("o_mod"), "_batch_id"), "OrderID")
          .join(s, "StoreID", "left").join(cu, "CustomerID", "left").join(pr, "ProductID", "left"))

gross = F.col("Quantity") * F.col("UnitPrice")
discount = F.round(gross * F.col("DiscountPct") / 100, 2)
store_sales = joined.select(
    F.col("OrderNumber").alias("order_number"), F.col("LineNumber").cast("int").alias("line_number"),
    F.col("OrderDate").alias("order_ts"), F.to_date("OrderDate").alias("order_date"),
    F.col("Channel").alias("channel"), F.lower(F.trim("PaymentMethod")).alias("payment_method"),
    F.upper(F.trim("StoreCode")).alias("store_code"), F.upper(F.trim("CustomerCode")).alias("customer_code"),
    F.upper(F.trim("SKU")).alias("sku"), F.col("Quantity").cast("int").alias("quantity"),
    F.col("UnitPrice").cast("decimal(10,2)").alias("unit_price_eur"),
    F.col("DiscountPct").cast("decimal(5,2)").alias("discount_pct"),
    F.round(gross, 2).cast("decimal(12,2)").alias("gross_amount_eur"),
    discount.cast("decimal(12,2)").alias("discount_amount_eur"),
    (F.round(gross, 2) - discount).cast("decimal(12,2)").alias("net_amount_eur"),
    F.lit("sqlserver").alias("source_system"),
    F.greatest("o_mod", "l_mod").alias("source_modified_at"),
    F.col("_batch_id").alias("bronze_batch_id"), F.current_timestamp().alias("silver_updated_at"),
)

merge_into_silver(store_sales, "store_sales_lines", ["order_number", "line_number"])
silver = spark.table(f"{SILVER}.store_sales_lines")
run_checks("store_sales_lines", {
    "row count equals bronze order lines": silver.count() == l.count(),
    "order_number + line_number is unique": is_unique(silver, ["order_number", "line_number"]),
    "every line found its store and product": no_nulls(silver, "store_code", "sku"),
    "net = gross - discount": silver.filter(
        "net_amount_eur <> gross_amount_eur - discount_amount_eur").limit(1).count() == 0,
})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Online sales lines (MongoDB)
# MAGIC Parse JSON → three date formats → deduplicate → explode items → standardise → convert to EUR.

# COMMAND ----------

ORDER_SCHEMA = T.StructType([
    T.StructField("orderNumber", T.StringType()), T.StructField("orderDate", T.StringType()),
    T.StructField("status", T.StringType()), T.StructField("currency", T.StringType()),
    T.StructField("guestEmail", T.StringType()),
    T.StructField("createdAt", T.StructType([T.StructField("$date", T.StringType())])),
    T.StructField("customer", T.StructType([T.StructField("code", T.StringType()),
                                            T.StructField("email", T.StringType()),
                                            T.StructField("country", T.StringType())])),
    T.StructField("shipping", T.StructType([T.StructField("city", T.StringType()),
                                            T.StructField("country", T.StringType()),
                                            T.StructField("cost", T.DoubleType())])),
    T.StructField("payment", T.StructType([T.StructField("method", T.StringType())])),
    T.StructField("items", T.ArrayType(T.StructType([
        T.StructField("line", T.IntegerType()), T.StructField("sku", T.StringType()),
        T.StructField("name", T.StringType()), T.StructField("qty", T.IntegerType()),
        T.StructField("unitPrice", T.DoubleType())]))),
])


def parse_order_date(col):
    inner = F.regexp_extract(col, r'"\$date"\s*:\s*"([^"]+)"', 1)
    text = F.when(F.trim(col).startswith("{"), inner).otherwise(F.trim(col))
    return F.coalesce(F.try_to_timestamp(text, F.lit("yyyy-MM-dd'T'HH:mm:ssX")),
                      F.try_to_timestamp(text, F.lit("yyyy-MM-dd'T'HH:mm:ss.SSSX")),
                      F.try_to_timestamp(text, F.lit("dd/MM/yyyy HH:mm")))


bronze = read_latest_batch("mongo_orders")
orders = (bronze.select(F.from_json("raw_json", ORDER_SCHEMA).alias("o"), "_batch_id").select("o.*", "_batch_id")
          .withColumn("order_ts", parse_order_date(F.col("orderDate")))
          .withColumn("created_ts", F.to_timestamp(F.col("createdAt.`$date`"))))
latest_first = Window.partitionBy("orderNumber").orderBy(F.col("created_ts").desc())
orders = orders.withColumn("_rn", F.row_number().over(latest_first)).filter("_rn = 1").drop("_rn")

lines = orders.select("*", F.explode("items").alias("item")).drop("items").select(
    F.upper(F.trim("orderNumber")).alias("order_number"), F.col("item.line").alias("line_number"),
    "order_ts", F.to_date("order_ts").alias("order_date"), F.lit("Web").alias("channel"),
    F.lower(F.trim("status")).alias("status"),
    F.coalesce(F.lower(F.trim("payment.method")), F.lit("unknown")).alias("payment_method"),
    F.upper(F.trim("customer.code")).alias("customer_code"), F.col("customer").isNull().alias("is_guest"),
    F.lower(F.coalesce("customer.email", "guestEmail")).alias("customer_email"),
    F.col("shipping.city").alias("ship_city"), F.upper("shipping.country").alias("ship_country"),
    F.upper(F.trim("item.sku")).alias("sku"), F.trim("item.name").alias("product_name"),
    F.col("item.qty").alias("quantity"), F.upper(F.trim("currency")).alias("currency"),
    F.col("item.unitPrice").cast("decimal(10,2)").alias("unit_price_local"),
    F.col("created_ts").alias("source_modified_at"), F.col("_batch_id").alias("bronze_batch_id"),
)

r = spark.table(f"{SILVER}.exchange_rates").select(
    F.col("rate_date").alias("order_date"), "currency", F.col("rate").alias("fx_rate"))
lines = (lines.join(r, ["order_date", "currency"], "left")
              .withColumn("fx_rate", F.when(F.col("currency") == "EUR", F.lit(1))
                                      .otherwise(F.col("fx_rate")).cast("decimal(12,6)")))
unit_eur = F.round(F.col("unit_price_local") / F.col("fx_rate"), 2)
online = lines.select(
    "order_number", "line_number", "order_ts", "order_date", "channel", "status", "payment_method",
    "customer_code", "is_guest", "customer_email", "ship_city", "ship_country", "sku", "product_name",
    "quantity", "currency", "unit_price_local", "fx_rate",
    unit_eur.cast("decimal(10,2)").alias("unit_price_eur"),
    F.round(unit_eur * F.col("quantity"), 2).cast("decimal(12,2)").alias("net_amount_eur"),
    F.lit("mongodb").alias("source_system"), "source_modified_at", "bronze_batch_id",
    F.current_timestamp().alias("silver_updated_at"),
)

merge_into_silver(online, "online_sales_lines", ["order_number", "line_number"])
silver = spark.table(f"{SILVER}.online_sales_lines")
unknown_skus = (silver.select("sku").distinct()
                .join(spark.table(f"{SILVER}.products").select("sku"), "sku", "left_anti").count())
run_checks("online_sales_lines", {
    "order_number + line_number is unique": is_unique(silver, ["order_number", "line_number"]),
    "every order date was parsed": no_nulls(silver, "order_ts"),
    "every line has an EUR price": no_nulls(silver, "fx_rate", "unit_price_eur"),
    "every SKU exists in silver.products": unknown_skus == 0,
})
documents, n_orders = bronze.count(), silver.select("order_number").distinct().count()
print(f"  info: {documents:,} documents -> {n_orders:,} orders ({documents - n_orders:,} duplicates removed) "
      f"-> {silver.count():,} lines")

# COMMAND ----------

# MAGIC %md ## Summary — the silver layer in Unity Catalog

# COMMAND ----------

display(spark.createDataFrame(
    [(t, spark.table(f"{SILVER}.{t}").count()) for t in
     ["customers", "products", "stores", "exchange_rates", "store_sales_lines", "online_sales_lines"]],
    "silver_table string, rows long"))
