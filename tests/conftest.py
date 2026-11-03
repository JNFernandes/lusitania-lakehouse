"""
Shared test setup (pytest loads this file automatically).

ONE Spark session for the whole test run: starting Spark takes a few seconds,
so every test reuses it.

It tries the real project session first (Delta enabled, see lusitania/spark.py).
If the Delta jars can't be downloaded (no internet), it falls back to plain Spark:
the unit tests still run, and the tests marked @pytest.mark.delta are skipped.
"""
import os
import sys
import warnings

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

_HAS_DELTA = False


@pytest.fixture(scope="session")
def spark():
    global _HAS_DELTA
    from pyspark.sql import SparkSession

    try:
        from lusitania.spark import get_spark
        session = get_spark("tests", cores="2")
        _HAS_DELTA = True
    except Exception as error:  # noqa: BLE001 - any start-up failure → plain Spark
        warnings.warn(f"Delta Lake not available, delta tests will be skipped: {error}", stacklevel=1)
        session = (SparkSession.builder.master("local[2]").appName("tests")
                   .config("spark.sql.shuffle.partitions", "2")
                   .config("spark.sql.session.timeZone", "UTC")
                   .config("spark.ui.enabled", "false")
                   .getOrCreate())
    session.conf.set("spark.sql.shuffle.partitions", "2")   # tiny data: few partitions = fast tests
    yield session
    session.stop()


@pytest.fixture
def delta_spark(spark):
    """The same session, for tests that need Delta (skipped when it is not available)."""
    if not _HAS_DELTA:
        pytest.skip("Delta Lake jars not available")
    return spark
