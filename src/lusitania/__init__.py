"""
Lusitânia Lakehouse: bronze → silver (PySpark + Delta Lake) → gold (dbt).

The package is organised by layer:

    lusitania.config     settings from .env
    lusitania.spark      the local Spark session (Delta enabled, Windows fixes)
    lusitania.log        logging setup
    lusitania.storage    WHERE tables live: Delta folders (laptop) or Unity Catalog (Databricks)
    lusitania.bronze     sources → raw Delta tables
    lusitania.silver     raw → clean, merged, checked Delta tables
    lusitania.pipeline   bronze → silver → dbt, in order
    lusitania.cli        the `lusitania` command
"""

__version__ = "0.2.0"
