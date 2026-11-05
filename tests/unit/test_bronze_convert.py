"""
SQL Server timestamps must reach bronze unchanged, whatever the computer's time zone.

The bug this guards against: a naive July timestamp became 09:00 on a laptop in Lisbon summer
time and 10:00 in a UTC container, so silver "updated" 1,731 unchanged customers.
"""
import datetime as dt
import os
import subprocess
import sys
import textwrap

from lusitania.bronze.convert import as_utc, rows_as_utc


def test_naive_datetime_is_marked_utc_with_the_same_wall_clock():
    value = as_utc(dt.datetime(2026, 7, 15, 10, 0))
    assert value.tzinfo is dt.timezone.utc
    assert (value.hour, value.minute) == (10, 0)


def test_other_values_are_unchanged():
    aware = dt.datetime(2026, 7, 15, 10, 0, tzinfo=dt.timezone.utc)
    for value in [None, 42, "C00010", dt.date(1990, 5, 1), aware]:
        assert as_utc(value) == value


def test_rows_keep_their_shape():
    rows = [("C00001", dt.datetime(2026, 1, 1, 8, 30), 3), ("C00002", None, 4)]
    result = rows_as_utc(rows)
    assert len(result) == 2 and all(isinstance(r, tuple) and len(r) == 3 for r in result)
    assert result[0][1].tzinfo is dt.timezone.utc
    assert result[1] == ("C00002", None, 4)


SPARK_SCRIPT = textwrap.dedent("""
    import datetime as dt, os, sys
    os.environ["PYSPARK_PYTHON"] = sys.executable
    from pyspark.sql import SparkSession, types as T
    from lusitania.bronze.convert import rows_as_utc
    spark = (SparkSession.builder.master("local[1]").config("spark.sql.session.timeZone", "UTC")
             .config("spark.ui.enabled", "false").getOrCreate())
    rows = rows_as_utc([(dt.datetime(2026, 1, 15, 10, 0),), (dt.datetime(2026, 7, 15, 10, 0),)])
    df = spark.createDataFrame(rows, T.StructType([T.StructField("t", T.TimestampType())]))
    print([r[0] for r in df.selectExpr("date_format(t, 'HH:mm')").collect()])
    spark.stop()
""")


def test_spark_stores_the_same_time_in_every_time_zone():
    """The real check: run Spark in a subprocess with different TZ settings (Linux/macOS only)."""
    if not hasattr(__import__("time"), "tzset"):
        import pytest
        pytest.skip("TZ can only be changed per process on Linux/macOS (CI runs this)")
    results = set()
    for zone in ["Europe/Lisbon", "UTC", "America/New_York"]:
        env = {**os.environ, "TZ": zone}
        out = subprocess.run([sys.executable, "-c", SPARK_SCRIPT], env=env, capture_output=True, text=True,
                             check=True).stdout.strip().splitlines()[-1]
        results.add(out)
    assert results == {"['10:00', '10:00']"}
