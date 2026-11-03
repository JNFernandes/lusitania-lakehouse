"""
silver/products  ←  bronze/sqlserver/product_product
                    + product_category + product_department

The three source tables are JOINED, so each product row carries its category
and department names. Silver stays one row per product; the star or snowflake
shape is decided later, in gold.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lusitania.silver.framework import (
    is_unique,
    merge_into_silver,
    no_nulls,
    none_where,
    read_latest_batch,
    run_checks,
)
from lusitania.storage import Lakehouse

TABLE = "products"
KEYS = ["sku"]


def clean(products: DataFrame, categories: DataFrame, departments: DataFrame) -> DataFrame:
    p = products.select(
        F.upper(F.trim("SKU")).alias("sku"),
        F.trim("ProductName").alias("product_name"),
        F.trim("Brand").alias("brand"),
        F.col("CategoryID").alias("category_id"),
        F.col("ListPrice").cast("decimal(10,2)").alias("list_price"),
        F.col("IsActive").alias("is_active"),
        F.col("ModifiedDate").alias("product_modified_at"),
        F.col("_batch_id").alias("bronze_batch_id"),
    )
    c = categories.select(
        F.col("CategoryID").alias("category_id"),
        F.trim("CategoryName").alias("category_name"),
        F.col("DepartmentID").alias("department_id"),
        F.col("ModifiedDate").alias("category_modified_at"),
    )
    d = departments.select(
        F.col("DepartmentID").alias("department_id"),
        F.trim("DepartmentName").alias("department_name"),
        F.col("ModifiedDate").alias("department_modified_at"),
    )

    # LEFT joins: a product whose category is missing is KEPT (with a null
    # category) so a check can catch it, instead of silently vanishing.
    df = p.join(c, "category_id", "left").join(d, "department_id", "left")

    return df.select(
        "sku", "product_name", "brand",
        "category_id", "category_name",
        "department_id", "department_name",
        "list_price", "is_active",
        # a product counts as changed when the product, its category or its department changed
        F.greatest("product_modified_at", "category_modified_at", "department_modified_at")
         .alias("source_modified_at"),
        "bronze_batch_id",
        F.current_timestamp().alias("silver_updated_at"),
    )


def build(lake: Lakehouse) -> None:
    products = read_latest_batch(lake, "sqlserver/product_product")
    categories = read_latest_batch(lake, "sqlserver/product_category")
    departments = read_latest_batch(lake, "sqlserver/product_department")
    merge_into_silver(lake, clean(products, categories, departments), TABLE, KEYS)

    silver = lake.read("silver", TABLE)
    run_checks(TABLE, {
        "row count equals bronze latest batch": silver.count() == products.count(),
        "sku is unique": is_unique(silver, KEYS),
        "every product found its category and department": no_nulls(silver, "category_name", "department_name"),
        "list_price is positive": none_where(silver, "list_price <= 0"),
    })
