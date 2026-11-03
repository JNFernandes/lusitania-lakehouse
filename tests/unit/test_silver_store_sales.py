"""clean() of silver/store_sales_lines: business keys instead of IDs, amounts in EUR."""
from datetime import datetime
from decimal import Decimal

import pytest

from lusitania.silver import store_sales


@pytest.fixture
def lines(spark):
    orders = spark.createDataFrame([
        (1, "SO000001", datetime(2026, 5, 10, 15, 30), 7, 42, "Store", " Card ", datetime(2026, 5, 10), "b1"),
        (2, "SO000002", datetime(2026, 5, 11, 10, 0), 7, None, "Store", "cash", datetime(2026, 5, 11), "b1"),
    ], "OrderID int, OrderNumber string, OrderDate timestamp, StoreID int, CustomerID int, Channel string, "
       "PaymentMethod string, ModifiedDate timestamp, _batch_id string")
    order_lines = spark.createDataFrame([
        (1, 1, 100, 3, Decimal("19.99"), Decimal("10.00"), datetime(2026, 5, 12)),
        (2, 1, 100, 1, Decimal("19.99"), Decimal("0.00"), datetime(2026, 5, 11)),
    ], "OrderID int, LineNumber int, ProductID int, Quantity int, UnitPrice decimal(10,2), "
       "DiscountPct decimal(5,2), ModifiedDate timestamp")
    stores = spark.createDataFrame([(7, "ls01")], "StoreID int, StoreCode string")
    customers = spark.createDataFrame([(42, "C00042")], "CustomerID int, CustomerCode string")
    products = spark.createDataFrame([(100, "SKU-00100")], "ProductID int, SKU string")
    result = store_sales.clean(orders, order_lines, stores, customers, products)
    return {r.order_number: r for r in result.collect()}


def test_ids_are_replaced_by_business_keys(lines):
    row = lines["SO000001"]
    assert (row.store_code, row.customer_code, row.sku) == ("LS01", "C00042", "SKU-00100")
    assert row.payment_method == "card"


def test_amounts_gross_discount_net(lines):
    row = lines["SO000001"]                      # 3 x 19.99 = 59.97, 10% discount = 6.00
    assert row.gross_amount_eur == Decimal("59.97")
    assert row.discount_amount_eur == Decimal("6.00")
    assert row.net_amount_eur == Decimal("53.97")


def test_walk_in_has_no_customer(lines):
    assert lines["SO000002"].customer_code is None


def test_line_changed_after_its_order_counts_as_changed(lines):
    assert lines["SO000001"].source_modified_at == datetime(2026, 5, 12)
