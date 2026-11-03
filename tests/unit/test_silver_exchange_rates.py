"""clean() of silver/exchange_rates: JSON → one row per day and currency, no gaps."""
import json
from datetime import date
from decimal import Decimal

import pytest

from lusitania.silver import exchange_rates


@pytest.fixture
def rates(spark):
    # Thursday 2 Jan and Friday 3 Jan published; Saturday 4 and Sunday 5 not; Monday 6 published
    response = {"base": "EUR", "rates": {
        "2025-01-02": {"GBP": 0.83, "USD": 1.03},
        "2025-01-03": {"GBP": 0.84, "USD": 1.04},
        "2025-01-06": {"GBP": 0.85, "USD": 1.05},
    }}
    bronze = spark.createDataFrame([(json.dumps(response), "b1")], "raw_json string, _batch_id string")
    return {(r.rate_date, r.currency): r for r in exchange_rates.clean(bronze).collect()}


def test_weekend_takes_the_previous_working_day(rates):
    saturday = rates[(date(2025, 1, 4), "GBP")]
    assert saturday.rate == Decimal("0.840000")
    assert saturday.is_published is False
    assert rates[(date(2025, 1, 5), "USD")].rate == Decimal("1.040000")


def test_published_days_keep_their_own_rate(rates):
    monday = rates[(date(2025, 1, 6), "GBP")]
    assert monday.rate == Decimal("0.850000")
    assert monday.is_published is True


def test_calendar_starts_on_1_january_with_the_first_rate(rates):
    # 1 January is a holiday: no earlier rate exists, so it takes the FIRST published one
    assert rates[(date(2025, 1, 1), "GBP")].rate == Decimal("0.830000")


def test_calendar_has_no_gaps_until_today(rates):
    days = {d for d, _ in rates}
    expected = (date.today() - date(2025, 1, 1)).days + 1
    assert len(days) == expected
    assert len(rates) == expected * 2          # two currencies
    assert all(r.rate is not None for r in rates.values())
