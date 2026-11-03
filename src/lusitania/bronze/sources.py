"""
The three SOURCES, each read into Spark DataFrames exactly as received.

    read_sqlserver(spark)        LusitaniaOLTP tables     → one DataFrame per table
    read_mongo(spark)            online orders            → one row per document, kept as raw JSON text
    read_exchange_rates(spark)   Frankfurter (ECB) API    → one row: the raw API response

This module only READS. Writing to bronze (and the metadata columns) is in
load.py, so a new source only needs a new reader here.
"""
from __future__ import annotations

import datetime as dt
import decimal
import json
from dataclasses import dataclass

import pyodbc
import requests
from bson import json_util
from pymongo import MongoClient
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import types as T

from lusitania import config

SQL_TABLES = [
    "product.Department",
    "product.Category",
    "product.Product",
    "sales.Store",
    "sales.Customer",
    "sales.SalesOrder",
    "sales.SalesOrderLine",
]


@dataclass
class Extract:
    """One extracted table: where it goes in bronze, the data, and where it came from."""
    table: str           # bronze name, e.g. "sqlserver/sales_customer"
    df: DataFrame
    source: str          # e.g. "sqlserver:LusitaniaOLTP.sales.Customer"


def spark_type(py_type, precision, scale) -> T.DataType:
    """Map the Python type pyodbc reports for a column to a Spark type (keeps source types in bronze)."""
    if py_type is bool:
        return T.BooleanType()
    if py_type is int:
        return T.LongType()
    if py_type is float:
        return T.DoubleType()
    if py_type is decimal.Decimal:
        return T.DecimalType(precision or 38, scale or 0)
    if py_type is dt.datetime:
        return T.TimestampType()
    if py_type is dt.date:
        return T.DateType()
    return T.StringType()


def read_sqlserver(spark: SparkSession) -> list[Extract]:
    extracts = []
    with pyodbc.connect(config.sql_connection_string()) as conn:
        cur = conn.cursor()
        for table in SQL_TABLES:
            cur.execute(f"SELECT * FROM {table}")
            schema = T.StructType([
                T.StructField(col[0], spark_type(col[1], col[4], col[5]), True)
                for col in cur.description
            ])
            df = spark.createDataFrame([tuple(r) for r in cur.fetchall()], schema)
            extracts.append(Extract(
                table="sqlserver/" + table.replace(".", "_").lower(),
                df=df,
                source=f"sqlserver:{config.OLTP_DATABASE}.{table}",
            ))
    return extracts


def read_mongo(spark: SparkSession) -> list[Extract]:
    client = MongoClient(config.MONGO_URI)
    try:
        docs = client[config.MONGO_DATABASE]["orders"].find({})
        # Keep each document whole, as JSON text: bronze must not depend on the
        # document structure, which can differ from one order to the next.
        rows = [(str(d["_id"]), json_util.dumps(d, json_options=json_util.RELAXED_JSON_OPTIONS))
                for d in docs]
    finally:
        client.close()

    schema = T.StructType([
        T.StructField("_id", T.StringType(), False),
        T.StructField("raw_json", T.StringType(), False),
    ])
    return [Extract("mongo/orders", spark.createDataFrame(rows, schema),
                    f"mongodb:{config.MONGO_DATABASE}.orders")]


def read_exchange_rates(spark: SparkSession) -> list[Extract]:
    end = dt.date.today().isoformat()
    url = f"{config.FX_API_URL}/{config.FX_START}..{end}"
    params = {"from": config.FX_BASE, "to": config.FX_SYMBOLS}

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()                           # stop loudly if the API fails
    json.loads(response.text)                             # make sure it is valid JSON

    schema = T.StructType([
        T.StructField("request_url", T.StringType(), False),
        T.StructField("status_code", T.IntegerType(), False),
        T.StructField("raw_json", T.StringType(), False),
    ])
    df = spark.createDataFrame([(response.url, response.status_code, response.text)], schema)
    return [Extract("api/exchange_rates", df, f"api:{config.FX_API_URL}")]


# name shown in the logs → reader
READERS = {
    "SQL Server": read_sqlserver,
    "MongoDB": read_mongo,
    "Exchange-rate API": read_exchange_rates,
}
