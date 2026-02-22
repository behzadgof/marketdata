"""Tests for marketdata.earnings_calendar module."""

from __future__ import annotations

from datetime import date
from pathlib import Path
import shutil
import uuid

import pytest

from marketdata.earnings_calendar import (
    EarningsCalendar,
    EarningsCallTime,
    EarningsContext,
    EarningsEvent,
    get_earnings_context,
    load_earnings_calendar,
)


class TestEarningsEvent:
    def test_bmo_reaction_day_same_day(self) -> None:
        event = EarningsEvent(
            symbol="AAPL",
            earnings_date=date(2024, 1, 15),
            call_time=EarningsCallTime.BMO,
        )
        assert event.get_reaction_day() == date(2024, 1, 15)

    def test_amc_reaction_day_next_day(self) -> None:
        event = EarningsEvent(
            symbol="AAPL",
            earnings_date=date(2024, 1, 15),
            call_time=EarningsCallTime.AMC,
        )
        assert event.get_reaction_day() == date(2024, 1, 16)


class TestEarningsContext:
    def test_no_earnings_factory(self) -> None:
        ctx = EarningsContext.no_earnings()
        assert ctx.is_earnings_reaction_day is False
        assert ctx.call_time is None
        assert ctx.days_since_earnings is None
        assert ctx.earnings_date is None

    def test_to_dict(self) -> None:
        ctx = EarningsContext(
            is_earnings_reaction_day=True,
            call_time=EarningsCallTime.BMO,
            days_since_earnings=0,
            earnings_date=date(2024, 1, 15),
        )
        data = ctx.to_dict()
        assert data["is_earnings_reaction_day"] is True
        assert data["call_time"] == "BMO"
        assert data["days_since_earnings"] == 0
        assert data["earnings_date"] == "2024-01-15"


class TestEarningsCalendar:
    def test_add_and_get_reaction_day_context(self) -> None:
        calendar = EarningsCalendar()
        calendar.add_event(
            EarningsEvent(
                symbol="AAPL",
                earnings_date=date(2024, 1, 15),
                call_time=EarningsCallTime.AMC,
            )
        )

        ctx = calendar.get_context("AAPL", date(2024, 1, 16))
        assert ctx.is_earnings_reaction_day is True
        assert ctx.call_time == EarningsCallTime.AMC

    def test_get_days_until_earnings(self) -> None:
        calendar = EarningsCalendar()
        calendar.add_event(
            EarningsEvent(
                symbol="AAPL",
                earnings_date=date(2024, 1, 15),
                call_time=EarningsCallTime.AMC,
            )
        )
        days = calendar.get_days_until_earnings("AAPL", date(2024, 1, 11))
        assert days == 5

    def test_serialization_roundtrip(self) -> None:
        calendar = EarningsCalendar()
        calendar.add_event(
            EarningsEvent(
                symbol="AAPL",
                earnings_date=date(2024, 1, 15),
                call_time=EarningsCallTime.AMC,
                fiscal_quarter="Q1",
                fiscal_year=2024,
            )
        )

        data = calendar.to_dict()
        loaded = EarningsCalendar.from_dict(data)
        ctx = loaded.get_context("AAPL", date(2024, 1, 16))
        assert ctx.is_earnings_reaction_day is True
        assert ctx.call_time == EarningsCallTime.AMC

    def test_save_and_load(self) -> None:
        calendar = EarningsCalendar()
        calendar.add_event(
            EarningsEvent(
                symbol="AAPL",
                earnings_date=date(2024, 1, 15),
                call_time=EarningsCallTime.AMC,
            )
        )

        temp_dir = Path(".tmp_test_cache") / f"earnings_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        path = temp_dir / "earnings.json"
        calendar.save(path)
        loaded = EarningsCalendar.load(path)
        ctx = loaded.get_context("AAPL", date(2024, 1, 16))
        assert ctx.is_earnings_reaction_day is True
        shutil.rmtree(temp_dir, ignore_errors=True)


class TestHelpers:
    def test_get_earnings_context_with_calendar(self) -> None:
        calendar = EarningsCalendar()
        calendar.add_event(
            EarningsEvent(
                symbol="AAPL",
                earnings_date=date(2024, 1, 15),
                call_time=EarningsCallTime.AMC,
            )
        )
        ctx = get_earnings_context("AAPL", date(2024, 1, 16), calendar)
        assert ctx.is_earnings_reaction_day is True

    def test_load_calendar_file_not_found(self) -> None:
        with pytest.raises(FileNotFoundError, match="Earnings calendar not found"):
            load_earnings_calendar(Path("/nonexistent/path/earnings.json"))
