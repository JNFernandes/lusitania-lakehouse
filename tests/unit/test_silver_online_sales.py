"""clean() of silver/online_sales_lines: the messy MongoDB JSON."""
import json
from datetime import date
from decimal import Decimal

import pytest
from pyspark.sql import functions as F

from lusitania.silver import online_sales


def order(number, order_date, created, items, currency="EUR", customer=True, payment=True, status="Delivered"):
    doc = {
        "orderNumber": number, "orderDate": order_date, "status": status, "currency": currency,
        "createdAt": {"$date": created},
        "shipping": {"city": "Porto", "country": "pt", "cost": 0.0},
        "items": items,
    }
    if customer:
        doc["customer"] = {"code": "c00010", "email": "Rui@Mail.PT", "country": "PT"}
    else:
        doc["guestEmail"] = "Guest1@Example.NET"
    if payment:
        doc["payment"] = {"method": "Card"}
    return json.dumps(doc)


def item(line, sku="SKU-00010", qty=1, price=10.0):
    return {"line": line, "sku": sku, "name": " Mouse ", "qty": qty, "unitPrice": price}


@pytest.fixture
def rates(spark):
    return spark.createDataFrame([(date(2025, 6, 10), "GBP", Decimal("0.850000"))],
                                 "rate_date date, currency string, rate decimal(12,6)")


def clean(spark, rates, *documents):
    bronze = spark.createDataFrame([(d, "b1") for d in documents], "raw_json string, _batch_id string")
    return online_sales.clean(bronze, rates).collect()


def parsed(spark, text):
    """parse_order_date() result as UTC text (collecting a timestamp would convert it to local time)."""
    df = spark.createDataFrame([(text,)], "d string")
    return df.select(F.date_format(online_sales.parse_order_date(F.col("d")), "yyyy-MM-dd HH:mm:ss")).first()[0]


@pytest.mark.parametrize("text", [
    "2025-06-10T14:32:00Z",                        # ISO 8601
    "2025-06-10T14:32:00.000Z",                    # ISO with milliseconds
    "10/06/2025 14:32",                            # Portuguese style
    '{"$date": "2025-06-10T14:32:00Z"}',           # a real BSON date, as JSON text
])
def test_every_date_format_gives_the_same_timestamp(spark, text):
    assert parsed(spark, text) == "2025-06-10 14:32:00"


def test_unknown_date_format_becomes_null(spark):
    assert parsed(spark, "June 10th") is None


def test_duplicate_order_keeps_the_latest_version(spark, rates):
    rows = clean(spark, rates,
                 order("WEB0000001", "2025-06-10T14:32:00Z", "2025-06-10T14:32:00Z", [item(1)], status="paid"),
                 order("WEB0000001", "2025-06-10T14:32:00Z", "2025-06-13T09:00:00Z", [item(1)], status="Delivered"))
    assert len(rows) == 1
    assert rows[0].status == "delivered"


def test_items_are_exploded_one_row_per_line(spark, rates):
    rows = clean(spark, rates, order("WEB0000002", "2025-06-10T14:32:00Z", "2025-06-10T14:32:00Z",
                                     [item(1), item(2, sku="sku-00020"), item(3)]))
    assert sorted(r.line_number for r in rows) == [1, 2, 3]
    assert {r.sku for r in rows} == {"SKU-00010", "SKU-00020"}          # lowercase SKU fixed


def test_guest_and_missing_payment(spark, rates):
    row = clean(spark, rates, order("WEB0000003", "2025-06-10T14:32:00Z", "2025-06-10T14:32:00Z",
                                    [item(1)], customer=False, payment=False))[0]
    assert row.is_guest is True
    assert row.customer_code is None
    assert row.customer_email == "guest1@example.net"
    assert row.payment_method == "unknown"


def test_registered_customer(spark, rates):
    row = clean(spark, rates, order("WEB0000004", "2025-06-10T14:32:00Z", "2025-06-10T14:32:00Z", [item(1)]))[0]
    assert (row.is_guest, row.customer_code, row.payment_method) == (False, "C00010", "card")


def test_eur_order_has_rate_one(spark, rates):
    row = clean(spark, rates, order("WEB0000005", "2025-06-10T14:32:00Z", "2025-06-10T14:32:00Z",
                                    [item(1, qty=2, price=10.0)]))[0]
    assert row.fx_rate == Decimal("1.000000")
    assert row.net_amount_eur == Decimal("20.00")


def test_gbp_order_is_converted_with_the_rate_of_its_date(spark, rates):
    # £17.00 at 0.85 GBP per EUR = €20.00 per unit
    row = clean(spark, rates, order("WEB0000006", "10/06/2025 18:00", "2025-06-10T18:00:00Z",
                                    [item(1, qty=3, price=17.0)], currency="gbp"))[0]
    assert row.currency == "GBP"
    assert row.fx_rate == Decimal("0.850000")
    assert row.unit_price_eur == Decimal("20.00")
    assert row.net_amount_eur == Decimal("60.00")
