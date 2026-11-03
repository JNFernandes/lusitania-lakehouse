"""clean() of the three dimension-like silver tables: customers, products, stores."""
from datetime import date, datetime

from lusitania.silver import customers, products, stores

CUSTOMER_COLUMNS = ("CustomerCode string, FirstName string, LastName string, Email string, Phone string, "
                    "City string, District string, Segment string, BirthDate date, CreatedDate timestamp, "
                    "ModifiedDate timestamp, _batch_id string")


def customer(code, city="Lisboa", modified=datetime(2026, 1, 1), first="ana", email="Ana@Mail.PT"):
    return (code, first, "silva", email, "910000000", city, "Lisboa", "Retail",
            date(1990, 5, 1), datetime(2025, 1, 1), modified, "b1")


def test_customers_are_trimmed_and_standardised(spark):
    bronze = spark.createDataFrame([customer(" c00001 ", first="  ana maria ", email=" Ana@Mail.PT ")],
                                   CUSTOMER_COLUMNS)
    row = customers.clean(bronze).first()
    assert row.customer_code == "C00001"
    assert row.first_name == "Ana Maria"
    assert row.last_name == "Silva"
    assert row.email == "ana@mail.pt"
    assert row.bronze_batch_id == "b1"


def test_customers_keep_the_most_recent_row_per_code(spark):
    bronze = spark.createDataFrame([
        customer("C00001", city="Porto", modified=datetime(2026, 1, 1)),
        customer("C00001", city="Évora", modified=datetime(2026, 3, 1)),     # newer
        customer("C00002"),
    ], CUSTOMER_COLUMNS)
    result = {r.customer_code: r.city for r in customers.clean(bronze).collect()}
    assert result == {"C00001": "Évora", "C00002": "Lisboa"}


def test_products_carry_category_and_department_names(spark):
    p = spark.createDataFrame(
        [("sku-00001 ", " Laptop ", "Acme", 10, 499.9, True, datetime(2026, 1, 1), "b1"),
         ("SKU-00002", "Ghost", "Acme", 99, 10.0, True, datetime(2026, 1, 1), "b1")],   # unknown category
        "SKU string, ProductName string, Brand string, CategoryID int, ListPrice double, IsActive boolean, "
        "ModifiedDate timestamp, _batch_id string")
    c = spark.createDataFrame([(10, " Computers ", 1, datetime(2026, 2, 1))],
                              "CategoryID int, CategoryName string, DepartmentID int, ModifiedDate timestamp")
    d = spark.createDataFrame([(1, "Electronics", datetime(2025, 6, 1))],
                              "DepartmentID int, DepartmentName string, ModifiedDate timestamp")

    rows = {r.sku: r for r in products.clean(p, c, d).collect()}

    laptop = rows["SKU-00001"]
    assert (laptop.product_name, laptop.category_name, laptop.department_name) == ("Laptop", "Computers", "Electronics")
    assert str(laptop.list_price) == "499.90"
    # changed when the product OR its category OR its department changed → the latest of the three
    assert laptop.source_modified_at == datetime(2026, 2, 1)
    # a product with an unknown category is kept, with NULL names, so the check can catch it
    assert rows["SKU-00002"].category_name is None


def test_stores_are_standardised(spark):
    bronze = spark.createDataFrame(
        [(" ls01 ", " Lisboa Colombo ", "Mall", "Lisboa", "Lisboa", date(2015, 3, 1), datetime(2026, 1, 1), "b1")],
        "StoreCode string, StoreName string, StoreType string, City string, District string, "
        "OpenDate date, ModifiedDate timestamp, _batch_id string")
    row = stores.clean(bronze).first()
    assert (row.store_code, row.store_name) == ("LS01", "Lisboa Colombo")
