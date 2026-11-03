"""
One place to create the LOCAL Spark session (on Databricks, `spark` already exists).

    from lusitania.spark import get_spark
    spark = get_spark("load_bronze")

It includes the fixes found while setting up Spark on Windows:
  - Spark's Python workers use THIS venv's Python (PYSPARK_PYTHON)
  - Delta Lake enabled (transactions, MERGE, time travel)
  - quieter logs (log4j2.properties next to this file)
"""
import os
import sys
from pathlib import Path

# Must be set BEFORE pyspark starts the Java process
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

_LOG_CONFIG = (Path(__file__).resolve().parent / "log4j2.properties").as_uri()


def get_spark(app_name: str = "lusitania", cores: str = "*") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")                         # run locally on this many CPU cores
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "8")       # the default 200 is too many for a laptop
        .config("spark.sql.session.timeZone", "UTC")       # store all timestamps in UTC
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")   # no progress bars mixed with the logs
        .config("spark.driver.extraJavaOptions", f"-Dlog4j2.configurationFile={_LOG_CONFIG}")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
