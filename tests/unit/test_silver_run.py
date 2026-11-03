"""
The WHOLE silver layer (all six build() functions, in order) on a tiny bronze.

Instead of Delta folders it uses MemoryLakehouse: a fake storage that keeps
tables in memory. This is possible because the silver code only talks to the
Lakehouse interface (see lusitania/storage.py), never to files directly.
A replacement like this is called a "test double" (or "fake").
"""
import json
from datetime import date, datetime
from decimal import Decimal

import pytest

from lusitania.silver.framework import DataQualityError
from lusitania.silver.run import run_silver
from lusitania.storage import Lakehouse


class MemoryLakehouse(Lakehouse):
    def __init__(self, spark):
        super().__init__(spark)
        self.data = {}

    def _keep(self, df):
        return self.spark.createDataFrame(df.collect(), df.schema)     # materialise: no lazy self-reference

    def name(self, layer, table):
        return f"memory:{layer}/{table}"

    def exists(self, layer, table):
        return (layer, table) in self.data

    def read(self, layer, table):
        return self.data[(layer, table)]

    def write(self, df, layer, table, mode="append"):
        if mode == "append" and self.exists(layer, table):
            df = self.read(layer, table).unionByName(df)
        self.data[(layer, table)] = self._keep(df)

    def merge(self, df, layer, table, keys):
        old = self.read(layer, table)
        inserted = df.join(old, keys, "left_anti").count()
        self.data[(layer, table)] = self._keep(old.join(df, keys, "left_anti").unionByName(df))
        return inserted, df.count() - inserted

    def history(self, layer, table):
        return self.spark.range(1)

    def tables(self, layer):
        return sorted(t for (lay, t) in self.data if lay == layer)


def _bronze(lake, table, rows, schema):
    lake.write(lake.spark.createDataFrame([(*r, "20261103_080000") for r in rows], schema + ", _batch_id string"),
               "bronze", table)


@pytest.fixture
def lake(spark):
    lake = MemoryLakehouse(spark)
    t = datetime(2026, 1, 1)
    _bronze(lake, "sqlserver/sales_customer",
            [(1, "C00001", "ana", "silva", "ana@mail.pt", "91", "Lisboa", "Lisboa", "Retail", date(1990, 1, 1), t, t)],
            "CustomerID int, CustomerCode string, FirstName string, LastName string, Email string, "
            "Phone string, City string, District string, Segment string, BirthDate date, "
            "CreatedDate timestamp, ModifiedDate timestamp")
    _bronze(lake, "sqlserver/product_department", [(1, "Electronics", t)],
            "DepartmentID int, DepartmentName string, ModifiedDate timestamp")
    _bronze(lake, "sqlserver/product_category", [(10, "Computers", 1, t)],
            "CategoryID int, CategoryName string, DepartmentID int, ModifiedDate timestamp")
    _bronze(lake, "sqlserver/product_product", [(100, "SKU-00100", "Mouse", "Acme", 10, Decimal("20.00"), True, t)],
            "ProductID int, SKU string, ProductName string, Brand string, CategoryID int, "
            "ListPrice decimal(10,2), IsActive boolean, ModifiedDate timestamp")
    _bronze(lake, "sqlserver/sales_store",
            [(7, "LS01", "Lisboa Colombo", "Mall", "Lisboa", "Lisboa", date(2015, 1, 1), t)],
            "StoreID int, StoreCode string, StoreName string, StoreType string, City string, District string, "
            "OpenDate date, ModifiedDate timestamp")
    _bronze(lake, "sqlserver/sales_salesorder", [(1, "SO000001", datetime(2026, 5, 10), 7, None, "Store", "card", t)],
            "OrderID int, OrderNumber string, OrderDate timestamp, StoreID int, CustomerID int, Channel string, "
            "PaymentMethod string, ModifiedDate timestamp")
    _bronze(lake, "sqlserver/sales_salesorderline", [(1, 1, 100, 2, Decimal("20.00"), Decimal("0.00"), t)],
            "OrderID int, LineNumber int, ProductID int, Quantity int, UnitPrice decimal(10,2), "
            "DiscountPct decimal(5,2), ModifiedDate timestamp")
    _bronze(lake, "api/exchange_rates",
            [(json.dumps({"base": "EUR", "rates": {"2025-01-02": {"GBP": 0.85}}}),)], "raw_json string")
    web_order = {"orderNumber": "WEB0000001", "orderDate": "2025-06-10T14:32:00Z", "status": "paid",
                 "currency": "GBP", "createdAt": {"$date": "2025-06-10T14:32:00Z"},
                 "customer": {"code": "C00001", "email": "ana@mail.pt", "country": "GB"},
                 "shipping": {"city": "London", "country": "GB", "cost": 0.0}, "payment": {"method": "card"},
                 "items": [{"line": 1, "sku": "sku-00100", "name": "Mouse", "qty": 1, "unitPrice": 17.0}]}
    _bronze(lake, "mongo/orders", [("id1", json.dumps(web_order))], "_id string, raw_json string")
    return lake


def test_whole_silver_layer_builds_and_passes_its_checks(lake):
    run_silver(lake)
    assert lake.tables("silver") == ["customers", "exchange_rates", "online_sales_lines",
                                     "products", "store_sales_lines", "stores"]
    web = lake.read("silver", "online_sales_lines").first()
    assert (web.sku, web.unit_price_eur) == ("SKU-00100", Decimal("20.00"))     # £17 / 0.85


def test_running_twice_changes_nothing(lake):
    run_silver(lake)
    before = {t: lake.read("silver", t).count() for t in lake.tables("silver")}
    run_silver(lake)
    assert {t: lake.read("silver", t).count() for t in lake.tables("silver")} == before


def test_a_failed_check_stops_the_run(lake):
    # a web order for a product that does not exist → "every SKU exists in silver/products" fails
    bad = lake.read("bronze", "mongo/orders").first().raw_json.replace("sku-00100", "SKU-99999")
    lake.data[("bronze", "mongo/orders")] = lake.spark.createDataFrame(
        [("id1", bad, "20261103_090000")], "_id string, raw_json string, _batch_id string")
    with pytest.raises(DataQualityError, match="every SKU exists"):
        run_silver(lake)
