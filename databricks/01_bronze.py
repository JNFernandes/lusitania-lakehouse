# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — files in the volume → bronze Delta tables
# MAGIC
# MAGIC **Input:** the 9 Parquet files uploaded to `/Volumes/<catalog>/landing/raw_files/`
# MAGIC (exported from the laptop with `export_for_databricks.py`).
# MAGIC
# MAGIC **Output:** one Delta table per file in `<catalog>.bronze`, e.g.
# MAGIC `lusitania.bronze.sqlserver_sales_customer`.
# MAGIC
# MAGIC Same bronze rules as on the laptop:
# MAGIC - **1:1 with the source** — no cleaning, no renaming
# MAGIC - **append**, never overwrite: every batch is kept
# MAGIC - **idempotent**: if a batch is already in the table, it is skipped, so running
# MAGIC   this notebook twice does not duplicate data
# MAGIC
# MAGIC The difference from the laptop: tables are registered in **Unity Catalog**
# MAGIC (`catalog.schema.table`) instead of being plain folders on disk.

# COMMAND ----------

# Widgets = notebook parameters (shown as boxes at the top of the notebook).
# Change the catalog here if you had to use "workspace" instead of "lusitania".
dbutils.widgets.text("catalog", "lusitania")
CATALOG = dbutils.widgets.get("catalog")

VOLUME = f"/Volumes/{CATALOG}/landing/raw_files"
print(f"Catalog: {CATALOG}\nFiles:   {VOLUME}")

# COMMAND ----------

# MAGIC %md ## 1. What is in the volume?

# COMMAND ----------

files = [f for f in dbutils.fs.ls(VOLUME) if f.name.endswith(".parquet")]
for f in files:
    print(f"{f.name:<45} {f.size / 1024:>10,.0f} KB")
print(f"\n{len(files)} files")

# COMMAND ----------

# MAGIC %md ## 2. Load each file into its bronze table

# COMMAND ----------

from pyspark.sql import functions as F

results = []
for f in files:
    # sqlserver__sales_customer.parquet  →  <catalog>.bronze.sqlserver_sales_customer
    table = f"{CATALOG}.bronze." + f.name.removesuffix(".parquet").replace("__", "_")

    df = spark.read.parquet(f.path)
    batch_id = df.select(F.max("_batch_id")).first()[0]

    if spark.catalog.tableExists(table):
        already_loaded = (spark.table(table)
                          .filter(F.col("_batch_id") == batch_id)
                          .limit(1).count() > 0)
        if already_loaded:
            results.append((table, batch_id, 0, "skipped (batch already loaded)"))
            continue

    rows = df.count()
    (df.write
       .format("delta")
       .mode("append")
       .option("mergeSchema", "true")
       .saveAsTable(table))
    results.append((table, batch_id, rows, "appended"))

display(spark.createDataFrame(results, "table string, batch_id string, rows long, action string"))

# COMMAND ----------

# MAGIC %md ## 3. Check: every bronze table, its rows and batches

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {CATALOG}.bronze"))

summary = []
for t in spark.sql(f"SHOW TABLES IN {CATALOG}.bronze").collect():
    name = f"{CATALOG}.bronze.{t.tableName}"
    df = spark.table(name)
    summary.append((t.tableName, df.count(), df.select("_batch_id").distinct().count()))

display(spark.createDataFrame(summary, "table string, rows long, batches long").orderBy("table"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Delta history — the same transaction log as on the laptop
# MAGIC
# MAGIC Every append is a version. Run the notebook a second time: the batch is
# MAGIC skipped, so no new version appears.

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {CATALOG}.bronze.sqlserver_sales_customer")
          .select("version", "timestamp", "operation", "operationMetrics"))
