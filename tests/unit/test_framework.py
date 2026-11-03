"""The shared silver steps: latest batch, checks, check helpers."""
import pytest

from lusitania.silver.framework import DataQualityError, is_unique, latest_batch, no_nulls, none_where, run_checks


def test_latest_batch_keeps_only_the_newest(spark):
    bronze = spark.createDataFrame(
        [("A", "20261101_080000"), ("A", "20261102_080000"), ("B", "20261102_080000")],
        "code string, _batch_id string")
    result = latest_batch(bronze)
    assert {r._batch_id for r in result.collect()} == {"20261102_080000"}
    assert result.count() == 2


def test_run_checks_passes_quietly():
    run_checks("t", {"a": True, "b": True})


def test_run_checks_raises_and_names_the_failed_check():
    with pytest.raises(DataQualityError, match="sku is unique"):
        run_checks("products", {"row count": True, "sku is unique": False})


def test_check_helpers(spark):
    df = spark.createDataFrame([("A", 1), ("A", 2), ("B", None)], "code string, qty int")
    assert not is_unique(df, ["code"])
    assert is_unique(df, ["code", "qty"])
    assert not no_nulls(df, "qty")
    assert no_nulls(df, "code")
    assert none_where(df, "qty > 5")
    assert not none_where(df, "qty >= 2")
