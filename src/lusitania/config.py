"""
Project settings, read from the .env file in the project folder.

Every module imports its settings from here instead of hard-coding server
names or paths, so moving to another machine means changing .env, not the code.
"""
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:  # e.g. on Databricks, where there is no .env and no python-dotenv
    load_dotenv = None

# src/lusitania/config.py → parents[2] is the project folder
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if load_dotenv is not None:
    load_dotenv(PROJECT_ROOT / ".env")

# --- SQL Server ----------------------------------------------------------------
SQL_SERVER = os.getenv("SQL_SERVER", "localhost")
SQL_DRIVER = os.getenv("SQL_DRIVER", "ODBC Driver 18 for SQL Server")
OLTP_DATABASE = os.getenv("OLTP_DATABASE", "LusitaniaOLTP")


def sql_connection_string(database: str = OLTP_DATABASE) -> str:
    """Windows authentication, same as SSMS. TrustServerCertificate for the local self-signed cert."""
    return (
        f"DRIVER={{{SQL_DRIVER}}};SERVER={SQL_SERVER};DATABASE={database};"
        "Trusted_Connection=yes;TrustServerCertificate=yes;"
    )


# --- MongoDB -------------------------------------------------------------------
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "lusitania_online")

# --- Exchange-rate API ---------------------------------------------------------
FX_API_URL = os.getenv("FX_API_URL", "https://api.frankfurter.app").rstrip("/")
FX_BASE = os.getenv("FX_BASE", "EUR")
FX_SYMBOLS = os.getenv("FX_SYMBOLS", "GBP,USD")
FX_START = os.getenv("FX_START", "2025-01-01")

# --- Folders -------------------------------------------------------------------
DATA_DIR = (PROJECT_ROOT / os.getenv("DATA_DIR", "data")).resolve()
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
EXPORT_DIR = DATA_DIR / "export"
GOLD_DATABASE = DATA_DIR / "lusitania_gold.duckdb"
DBT_DIR = PROJECT_ROOT / "dbt"
