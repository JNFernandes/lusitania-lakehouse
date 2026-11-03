"""
Runs the whole silver layer, in dependency order:

    customers, products, stores, exchange_rates   (independent, from bronze only)
    store_sales_lines                             (from bronze)
    online_sales_lines                            (needs silver exchange_rates and products)

Like the Load_warehouse package in Project 1: if a table's checks fail,
DataQualityError stops the run and the following tables are not built.
"""
from __future__ import annotations

import logging
import time

from lusitania.silver import customers, exchange_rates, online_sales, products, store_sales, stores
from lusitania.storage import Lakehouse

log = logging.getLogger(__name__)

STEPS = [customers, products, stores, exchange_rates, store_sales, online_sales]


def run_silver(lake: Lakehouse) -> None:
    started = time.time()
    for step in STEPS:
        step_started = time.time()
        log.info("silver/%s", step.TABLE)
        step.build(lake)
        log.info("silver/%s done in %.0fs", step.TABLE, time.time() - step_started)
    log.info("silver layer complete in %.0fs", time.time() - started)
