"""
WHERE the lakehouse tables live.

The pipeline never says "a folder on disk" or "a Unity Catalog table". It asks
a Lakehouse object for a table by LAYER and NAME, e.g. ("silver", "customers"):

    LocalLakehouse(spark, "data")          laptop:      data/silver/customers        (a Delta folder)
    CatalogLakehouse(spark, "lusitania")   Databricks:  lusitania.silver.customers   (a Unity Catalog table)

So the same bronze and silver code runs in both places. Only the object
passed in changes. (This is the "dependency injection" idea: the code
receives what it depends on instead of creating it itself.)

Names can contain a "/" to group tables by source: ("bronze", "sqlserver/sales_customer")
is the folder data/bronze/sqlserver/sales_customer on the laptop and the table
lusitania.bronze.sqlserver_sales_customer on Databricks.
"""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger(__name__)

# Columns that change on every run; a difference in them alone is not a real change.
AUDIT_COLUMNS = ("bronze_batch_id", "silver_updated_at")


def merge_conditions(columns: list[str], keys: list[str],
                     ignore: tuple[str, ...] = AUDIT_COLUMNS) -> tuple[str, str]:
    """
    The two conditions of an idempotent MERGE, as SQL text:

        on      t.key = s.key [AND ...]                 which target row matches a source row
        changed NOT (t.col <=> s.col) [OR ...]          update only if a business column changed

    <=> is the null-safe "equals": NULL <=> NULL is true, so a column that is NULL
    on both sides does not count as a change.
    """
    if not keys:
        raise ValueError("a MERGE needs at least one key column")
    on = " AND ".join(f"t.{k} = s.{k}" for k in keys)
    compare = [c for c in columns if c not in keys and c not in ignore]
    changed = " OR ".join(f"NOT (t.{c} <=> s.{c})" for c in compare) or "false"
    return on, changed


class Lakehouse(ABC):
    """What every storage location must be able to do."""

    def __init__(self, spark: SparkSession):
        self.spark = spark

    @abstractmethod
    def name(self, layer: str, table: str) -> str:
        """Where the table is, as text (for logs and messages)."""

    @abstractmethod
    def exists(self, layer: str, table: str) -> bool: ...

    @abstractmethod
    def read(self, layer: str, table: str) -> DataFrame: ...

    @abstractmethod
    def write(self, df: DataFrame, layer: str, table: str, mode: str = "append") -> None:
        """mode: "append" adds rows; "errorifexists" creates a new table."""

    @abstractmethod
    def merge(self, df: DataFrame, layer: str, table: str, keys: list[str]) -> tuple[int, int]:
        """Upsert df on the keys. Returns (rows inserted, rows updated)."""

    @abstractmethod
    def history(self, layer: str, table: str) -> DataFrame:
        """The Delta transaction log: one row per version."""

    @abstractmethod
    def tables(self, layer: str) -> list[str]:
        """Names of the tables in a layer."""


class LocalLakehouse(Lakehouse):
    """Delta tables as folders under data_dir (the laptop)."""

    def __init__(self, spark: SparkSession, data_dir: str | Path):
        super().__init__(spark)
        self.data_dir = Path(data_dir)

    def path(self, layer: str, table: str) -> Path:
        return self.data_dir / layer / table

    def name(self, layer, table):
        return f"{layer}/{table}"

    def exists(self, layer, table):
        from delta.tables import DeltaTable
        return DeltaTable.isDeltaTable(self.spark, str(self.path(layer, table)))

    def read(self, layer, table):
        return self.spark.read.format("delta").load(str(self.path(layer, table)))

    def write(self, df, layer, table, mode="append"):
        (df.write.format("delta")
           .mode(mode)
           .option("mergeSchema", "true")     # a new source column is added instead of failing
           .save(str(self.path(layer, table))))

    def merge(self, df, layer, table, keys):
        from delta.tables import DeltaTable
        on, changed = merge_conditions(df.columns, keys)
        target = DeltaTable.forPath(self.spark, str(self.path(layer, table)))
        (target.alias("t")
            .merge(df.alias("s"), on)
            .whenMatchedUpdateAll(condition=changed)
            .whenNotMatchedInsertAll()
            .execute())
        metrics = target.history(1).select("operationMetrics").first()[0]
        return int(metrics.get("numTargetRowsInserted", 0)), int(metrics.get("numTargetRowsUpdated", 0))

    def history(self, layer, table):
        from delta.tables import DeltaTable
        return DeltaTable.forPath(self.spark, str(self.path(layer, table))).history()

    def tables(self, layer):
        base = self.data_dir / layer
        return sorted(p.parent.relative_to(base).as_posix() for p in base.glob("**/_delta_log"))


class CatalogLakehouse(Lakehouse):
    """Delta tables registered in Unity Catalog as <catalog>.<layer>.<table> (Databricks)."""

    def __init__(self, spark: SparkSession, catalog: str):
        super().__init__(spark)
        self.catalog = catalog

    def name(self, layer, table):
        return f"{self.catalog}.{layer}.{table.replace('/', '_')}"

    def exists(self, layer, table):
        return self.spark.catalog.tableExists(self.name(layer, table))

    def read(self, layer, table):
        return self.spark.table(self.name(layer, table))

    def write(self, df, layer, table, mode="append"):
        (df.write.format("delta")
           .mode(mode)
           .option("mergeSchema", "true")
           .saveAsTable(self.name(layer, table)))

    def merge(self, df, layer, table, keys):
        # SQL MERGE: works on serverless compute and returns its own row counts
        on, changed = merge_conditions(df.columns, keys)
        view = f"merge_source_{table.replace('/', '_')}"
        df.createOrReplaceTempView(view)
        result = self.spark.sql(f"""
            MERGE INTO {self.name(layer, table)} AS t
            USING {view} AS s
            ON {on}
            WHEN MATCHED AND ({changed}) THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """).first()
        return int(result["num_inserted_rows"]), int(result["num_updated_rows"])

    def history(self, layer, table):
        return self.spark.sql(f"DESCRIBE HISTORY {self.name(layer, table)}")

    def tables(self, layer):
        rows = self.spark.sql(f"SHOW TABLES IN {self.catalog}.{layer}").collect()
        return sorted(r.tableName for r in rows)
