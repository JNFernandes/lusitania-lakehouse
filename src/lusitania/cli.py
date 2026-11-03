"""
The `lusitania` command (installed by `pip install -e .`, works from any folder):

    lusitania run                      bronze → silver → gold, stops at the first failure
    lusitania bronze                   load a new bronze batch from the three sources
    lusitania silver                   rebuild silver from the latest bronze batch
    lusitania gold [--target databricks] [--command test]
                                       dbt build (or another dbt command) in the dbt folder
    lusitania inspect                  rows, batches and versions of every bronze table

Add -v for DEBUG logging, e.g.  lusitania -v silver
"""
from __future__ import annotations

import argparse
import logging
import sys

from lusitania import __version__, pipeline
from lusitania.log import setup_logging

log = logging.getLogger("lusitania")


def _inspect(_args) -> None:
    from lusitania.bronze.report import inspect_bronze
    lake = pipeline._local_lake("inspect")
    inspect_bronze(lake)
    lake.spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="lusitania", description="Lusitânia Lakehouse pipeline")
    parser.add_argument("-v", "--verbose", action="store_true", help="DEBUG logging")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    sub = parser.add_subparsers(dest="command", required=True, metavar="COMMAND")

    run = sub.add_parser("run", help="bronze → silver → gold")
    run.add_argument("--target", default="local", choices=["local", "databricks"], help="dbt target for gold")
    run.set_defaults(func=lambda a: pipeline.run_all(a.target))

    sub.add_parser("bronze", help="load a new bronze batch").set_defaults(func=lambda a: pipeline.run_bronze())
    sub.add_parser("silver", help="build silver from bronze").set_defaults(func=lambda a: pipeline.run_silver())

    gold = sub.add_parser("gold", help="run dbt in the dbt folder")
    gold.add_argument("--target", default="local", choices=["local", "databricks"])
    gold.add_argument("--command", default="build", help="dbt command: build, run, test, snapshot, docs ...")
    gold.set_defaults(func=lambda a: pipeline.run_gold(a.target, a.command))

    sub.add_parser("inspect", help="look inside bronze").set_defaults(func=_inspect)
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    setup_logging("DEBUG" if args.verbose else "INFO")
    try:
        args.func(args)
    except Exception as error:               # one clear line + exit code 1 (CI and schedulers check it)
        log.error("FAILED: %s", error)
        log.debug("details", exc_info=True)
        sys.exit(1)
