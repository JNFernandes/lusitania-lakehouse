"""
Logging for the whole project, instead of print().

Each module creates its own logger:

    import logging
    log = logging.getLogger(__name__)      # e.g. "lusitania.silver.customers"
    log.info("merged %s rows", n)

and the entry point (the CLI, or a Databricks notebook) calls setup_logging() once.
Every line then gets a time, a level and the module it came from:

    22:41:07  INFO     lusitania.silver.framework   merged into silver/customers: 0 inserted, 3 updated

Why it is better than print: levels (DEBUG / INFO / WARNING / ERROR) can be
filtered, the source of each line is visible, and the same messages can later
go to a file or a monitoring tool without changing the code.
"""
import logging
import sys

FORMAT = "%(asctime)s  %(levelname)-8s %(name)-30s %(message)s"


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(level=level.upper(), format=FORMAT, datefmt="%H:%M:%S",
                        stream=sys.stdout, force=True)
    # libraries that are too talkative at INFO
    for noisy in ("py4j", "urllib3", "pymongo"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
