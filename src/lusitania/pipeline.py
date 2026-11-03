"""
The whole lakehouse pipeline, the equivalent of Master.dtsx in Project 1:

    1. bronze   sources → raw Delta tables                      (Spark)
    2. silver   clean, merged, checked Delta tables             (Spark)
    3. gold     snapshots (SCD2), dimensions, fact, tests       (dbt)

Each step runs only if the previous one succeeded: a failed silver check
raises DataQualityError and gold is never built on bad data.
"""
from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

from lusitania import config

log = logging.getLogger(__name__)


def _local_lake(app_name: str):
    """The Spark session + the laptop's Delta folders (imported here: Spark starts only when needed)."""
    from lusitania.spark import get_spark
    from lusitania.storage import LocalLakehouse
    return LocalLakehouse(get_spark(app_name), config.DATA_DIR)


def run_bronze(lake=None) -> None:
    from lusitania.bronze.load import load_bronze
    lake = lake or _local_lake("bronze")
    load_bronze(lake)


def run_silver(lake=None) -> None:
    from lusitania.silver.run import run_silver as build_silver
    lake = lake or _local_lake("silver")
    build_silver(lake)


def dbt_executable() -> str:
    """dbt from THIS venv (next to python.exe), else whatever is on the PATH."""
    venv_dbt = Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt")
    return str(venv_dbt) if venv_dbt.exists() else (shutil.which("dbt") or "dbt")


def run_gold(target: str = "local", command: str = "build") -> None:
    """dbt runs from its own folder, where dbt_project.yml and profiles.yml are."""
    args = [dbt_executable(), command, "--target", target]
    log.info("gold: dbt %s --target %s  (in %s)", command, target, config.DBT_DIR)
    result = subprocess.run(args, cwd=config.DBT_DIR)
    if result.returncode != 0:
        raise RuntimeError(f"dbt {command} failed (exit code {result.returncode})")


def run_all(target: str = "local") -> None:
    started = time.time()
    lake = _local_lake("pipeline")            # ONE Spark session for bronze and silver
    for name, step in [("bronze", run_bronze), ("silver", run_silver)]:
        log.info("=" * 20 + " STEP: %s " + "=" * 20, name)
        step(lake)
    lake.spark.stop()                         # free the memory before dbt starts

    log.info("=" * 20 + " STEP: gold " + "=" * 20)
    run_gold(target)
    log.info("pipeline complete in %.0fs. Look at the result with:  python scripts/look_gold.py",
             time.time() - started)
