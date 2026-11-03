"""The MERGE conditions and table names: plain Python, no Spark needed."""
import pytest

from lusitania.storage import CatalogLakehouse, LocalLakehouse, merge_conditions


def test_merge_conditions_join_on_every_key():
    on, _ = merge_conditions(["order_number", "line_number", "quantity"], ["order_number", "line_number"])
    assert on == "t.order_number = s.order_number AND t.line_number = s.line_number"


def test_merge_conditions_compare_business_columns_null_safe():
    _, changed = merge_conditions(["sku", "list_price", "brand"], ["sku"])
    assert changed == "NOT (t.list_price <=> s.list_price) OR NOT (t.brand <=> s.brand)"


def test_merge_conditions_ignore_audit_columns():
    # bronze_batch_id and silver_updated_at change on every run: not a real change
    _, changed = merge_conditions(["sku", "list_price", "bronze_batch_id", "silver_updated_at"], ["sku"])
    assert "bronze_batch_id" not in changed
    assert "silver_updated_at" not in changed


def test_merge_conditions_only_keys_never_update():
    _, changed = merge_conditions(["sku"], ["sku"])
    assert changed == "false"


def test_merge_conditions_need_a_key():
    with pytest.raises(ValueError):
        merge_conditions(["sku"], [])


def test_local_and_catalog_names_point_to_the_same_table():
    local = LocalLakehouse(spark=None, data_dir="data")
    catalog = CatalogLakehouse(spark=None, catalog="lusitania")
    assert local.path("bronze", "sqlserver/sales_customer").as_posix() == "data/bronze/sqlserver/sales_customer"
    assert catalog.name("bronze", "sqlserver/sales_customer") == "lusitania.bronze.sqlserver_sales_customer"
    assert catalog.name("silver", "customers") == "lusitania.silver.customers"
