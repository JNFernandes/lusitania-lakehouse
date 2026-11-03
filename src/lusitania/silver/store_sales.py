"""
silver/store_sales_lines  ←  bronze/sqlserver/ sales_salesorder, sales_salesorderline,
                                               sales_store, sales_customer, product_product

Grain: ONE ROW PER ORDER LINE (same grain as fact.Sales in Project 1).

The source tables link to each other with internal IDs (StoreID, CustomerID,
ProductID). Silver replaces them with BUSINESS KEYS (store_code, customer_code,
sku), because those are the keys every source shares: the MongoDB orders only
know customer codes and SKUs, never SQL Server IDs.

Amounts are calculated here (gross, discount, net), in EUR.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lusitania.silver.framework import is_unique, merge_into_silver, no_nulls, none_where, read_latest_batch, run_checks
from lusitania.storage import Lakehouse

TABLE = "store_sales_lines"
KEYS = ["order_number", "line_number"]


def clean(orders: DataFrame, lines: DataFrame, stores: DataFrame,
          customers: DataFrame, products: DataFrame) -> DataFrame:
    o = orders.select("OrderID", "OrderNumber", "OrderDate", "StoreID", "CustomerID",
                      "Channel", "PaymentMethod", F.col("ModifiedDate").alias("order_modified"),
                      "_batch_id")
    ln = lines.select("OrderID", "LineNumber", "ProductID", "Quantity", "UnitPrice", "DiscountPct",
                      F.col("ModifiedDate").alias("line_modified"))
    s = stores.select("StoreID", "StoreCode")
    c = customers.select("CustomerID", "CustomerCode")
    p = products.select("ProductID", "SKU")

    df = (ln.join(o, "OrderID", "inner")         # every line belongs to an order
            .join(s, "StoreID", "left")          # left joins: a missing match is caught by the checks
            .join(c, "CustomerID", "left")       # CustomerID is NULL for walk-ins → customer_code NULL
            .join(p, "ProductID", "left"))

    gross = F.col("Quantity") * F.col("UnitPrice")
    discount = F.round(gross * F.col("DiscountPct") / 100, 2)

    return df.select(
        F.col("OrderNumber").alias("order_number"),
        F.col("LineNumber").cast("int").alias("line_number"),
        F.col("OrderDate").alias("order_ts"),
        F.to_date("OrderDate").alias("order_date"),
        F.col("Channel").alias("channel"),
        F.lower(F.trim("PaymentMethod")).alias("payment_method"),
        F.upper(F.trim("StoreCode")).alias("store_code"),
        F.upper(F.trim("CustomerCode")).alias("customer_code"),
        F.upper(F.trim("SKU")).alias("sku"),
        F.col("Quantity").cast("int").alias("quantity"),
        F.col("UnitPrice").cast("decimal(10,2)").alias("unit_price_eur"),
        F.col("DiscountPct").cast("decimal(5,2)").alias("discount_pct"),
        F.round(gross, 2).cast("decimal(12,2)").alias("gross_amount_eur"),
        discount.cast("decimal(12,2)").alias("discount_amount_eur"),
        (F.round(gross, 2) - discount).cast("decimal(12,2)").alias("net_amount_eur"),
        F.lit("sqlserver").alias("source_system"),
        F.greatest("order_modified", "line_modified").alias("source_modified_at"),
        F.col("_batch_id").alias("bronze_batch_id"),
        F.current_timestamp().alias("silver_updated_at"),
    )


def build(lake: Lakehouse) -> None:
    src = {name: read_latest_batch(lake, f"sqlserver/{name}") for name in
           ["sales_salesorder", "sales_salesorderline", "sales_store", "sales_customer", "product_product"]}
    df = clean(src["sales_salesorder"], src["sales_salesorderline"], src["sales_store"],
               src["sales_customer"], src["product_product"])
    merge_into_silver(lake, df, TABLE, KEYS)

    silver = lake.read("silver", TABLE)
    run_checks(TABLE, {
        "row count equals bronze order lines": silver.count() == src["sales_salesorderline"].count(),
        "order_number + line_number is unique": is_unique(silver, KEYS),
        "every line found its store and product": no_nulls(silver, "store_code", "sku"),
        "quantity is positive": none_where(silver, "quantity <= 0"),
        "net = gross - discount": none_where(silver, "net_amount_eur <> gross_amount_eur - discount_amount_eur"),
    })
