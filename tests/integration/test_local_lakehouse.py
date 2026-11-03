"""
The silver MERGE against real Delta tables in a temporary folder.

Integration test: it needs Delta Lake (the jars Spark downloads the first time),
so it is marked "delta" and skipped automatically when Delta is not available.
"""
import pytest

from lusitania.silver.framework import merge_into_silver
from lusitania.storage import LocalLakehouse

pytestmark = pytest.mark.delta

SCHEMA = "sku string, list_price int, bronze_batch_id string"


@pytest.fixture
def lake(delta_spark, tmp_path):
    return LocalLakehouse(delta_spark, tmp_path)


def test_first_run_creates_the_table(lake):
    df = lake.spark.createDataFrame([("A", 10, "b1"), ("B", 20, "b1")], SCHEMA)
    merge_into_silver(lake, df, "products", ["sku"])
    assert lake.exists("silver", "products")
    assert lake.read("silver", "products").count() == 2


def test_merge_is_idempotent_and_detects_changes(lake):
    first = lake.spark.createDataFrame([("A", 10, "b1"), ("B", 20, "b1")], SCHEMA)
    merge_into_silver(lake, first, "products", ["sku"])

    # same data, new batch id → nothing to do (batch id is an audit column)
    same = lake.spark.createDataFrame([("A", 10, "b2"), ("B", 20, "b2")], SCHEMA)
    assert lake.merge(same, "silver", "products", ["sku"]) == (0, 0)

    # one price changed, one new product
    changed = lake.spark.createDataFrame([("A", 11, "b3"), ("B", 20, "b3"), ("C", 30, "b3")], SCHEMA)
    assert lake.merge(changed, "silver", "products", ["sku"]) == (1, 1)

    prices = {r.sku: r.list_price for r in lake.read("silver", "products").collect()}
    assert prices == {"A": 11, "B": 20, "C": 30}
    assert lake.history("silver", "products").count() >= 2      # every write is a Delta version


def test_append_keeps_every_batch(lake):
    for batch in ["b1", "b2"]:
        lake.write(lake.spark.createDataFrame([("A", 1, batch)], SCHEMA), "bronze", "sqlserver/product")
    assert lake.tables("bronze") == ["sqlserver/product"]
    assert lake.read("bronze", "sqlserver/product").count() == 2
