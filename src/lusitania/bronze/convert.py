"""
Make source values mean the same thing on every computer.

SQL Server `datetime` columns have NO time zone: pyodbc returns them as "naive" Python
datetimes (just "2026-07-15 10:00"). When Spark receives a naive datetime it assumes it is
in the COMPUTER's time zone and converts it. So the same row became 09:00 on a laptop in
Lisbon summer time (UTC+1) and 10:00 in a Linux container (UTC): the result depended on
where the pipeline ran.

Marking every naive datetime as UTC before Spark sees it removes the guess: Spark stores
exactly the wall-clock value SQL Server has, on any machine (the Spark session itself also
runs in UTC, see lusitania/spark.py).
"""
from __future__ import annotations

import datetime as dt
from collections.abc import Iterable


def as_utc(value):
    """A naive datetime → the same wall-clock time, marked UTC. Anything else unchanged."""
    if isinstance(value, dt.datetime) and value.tzinfo is None:
        return value.replace(tzinfo=dt.timezone.utc)
    return value


def rows_as_utc(rows: Iterable[Iterable]) -> list[tuple]:
    """Apply as_utc() to every value of every row (e.g. the rows of cursor.fetchall())."""
    return [tuple(as_utc(v) for v in row) for row in rows]
