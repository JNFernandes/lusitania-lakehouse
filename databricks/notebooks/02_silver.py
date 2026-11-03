# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: bronze tables → clean silver tables
# MAGIC
# MAGIC **Input:** `<catalog>.bronze.*` (loaded by `01_bronze`)
# MAGIC **Output:** six Delta tables in `<catalog>.silver`
# MAGIC
# MAGIC The cleaning logic is **not written in this notebook**. It is the same Python
# MAGIC package the laptop runs, `src/lusitania/silver/`, imported from this Git folder.
# MAGIC One version of the code, tested by `pytest`, used in both places.
# MAGIC
# MAGIC The only difference is WHERE the tables are, and that is one line:
# MAGIC
# MAGIC | | storage object | a table is |
# MAGIC |---|---|---|
# MAGIC | laptop | `LocalLakehouse(spark, "data")` | the folder `data/silver/customers` |
# MAGIC | Databricks | `CatalogLakehouse(spark, "lusitania")` | the table `lusitania.silver.customers` |
# MAGIC
# MAGIC **Requirement:** this notebook must run from a **Git folder** (Workspace → Create →
# MAGIC Git folder → the GitHub repository), so that `src/` is next to it.

# COMMAND ----------

dbutils.widgets.text("catalog", "lusitania")
CATALOG = dbutils.widgets.get("catalog")
print(f"{CATALOG}.bronze  →  {CATALOG}.silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import the project package
# MAGIC
# MAGIC This notebook is in `<repo>/databricks/notebooks/`, so the package is two folders up,
# MAGIC in `<repo>/src`. Adding that folder to `sys.path` makes `import lusitania` work.
# MAGIC
# MAGIC After pulling new code into the Git folder, restart Python
# MAGIC (`dbutils.library.restartPython()`) so the new version is imported.

# COMMAND ----------

import os
import sys

SRC = os.path.abspath("../../src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)

from lusitania.log import setup_logging
from lusitania.silver.run import run_silver
from lusitania.storage import CatalogLakehouse

setup_logging()
print(f"package imported from {SRC}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build the silver layer
# MAGIC
# MAGIC Six tables, in dependency order. For each one: **READ** the latest bronze batch →
# MAGIC **CLEAN** → **MERGE** into silver (idempotent) → **CHECK**. A failed check raises
# MAGIC `DataQualityError` and stops the notebook, so gold is never built on bad data.

# COMMAND ----------

lake = CatalogLakehouse(spark, CATALOG)
run_silver(lake)

# COMMAND ----------

# MAGIC %md ## 3. Summary: the silver layer in Unity Catalog

# COMMAND ----------

display(spark.createDataFrame(
    [(t, lake.read("silver", t).count()) for t in lake.tables("silver")],
    "silver_table string, rows long"))
