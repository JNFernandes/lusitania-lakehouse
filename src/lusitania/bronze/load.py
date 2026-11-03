"""
Loads the BRONZE layer: every source extract, appended as a new batch.

Bronze rules followed here:
  - 1:1 with the source: no renaming, no cleaning, no joins
  - APPEND only: every run adds a new batch and old batches are kept,
    so silver and gold can always be rebuilt from bronze
  - every row gets metadata columns:
        _batch_id     which run loaded it
        _ingested_at  when (UTC)
        _source       where it came from
"""
from __future__ import annotations

import datetime as dt
import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lusitania.bronze.sources import READERS
from lusitania.storage import Lakehouse

log = logging.getLogger(__name__)


def new_batch() -> tuple[str, dt.datetime]:
    """A batch id from the current UTC time, e.g. 20261103_224107."""
    ingested_at = dt.datetime.now(dt.timezone.utc).replace(tzinfo=None)
    return ingested_at.strftime("%Y%m%d_%H%M%S"), ingested_at


def add_metadata(df: DataFrame, batch_id: str, ingested_at: dt.datetime, source: str) -> DataFrame:
    return (df.withColumn("_batch_id", F.lit(batch_id))
              .withColumn("_ingested_at", F.lit(ingested_at).cast("timestamp"))
              .withColumn("_source", F.lit(source)))


def load_bronze(lake: Lakehouse, readers: dict = READERS) -> dict[str, int]:
    """Read every source and append it to bronze. Returns {table: rows written}."""
    batch_id, ingested_at = new_batch()
    log.info("bronze load, batch %s", batch_id)

    counts = {}
    for source_name, read in readers.items():
        log.info("reading %s", source_name)
        for extract in read(lake.spark):
            df = add_metadata(extract.df, batch_id, ingested_at, extract.source)
            lake.write(df, "bronze", extract.table, mode="append")
            counts[extract.table] = df.count()
            log.info("  %-30s %8s rows -> %s", extract.table, f"{counts[extract.table]:,}",
                     lake.name("bronze", extract.table))
    return counts
