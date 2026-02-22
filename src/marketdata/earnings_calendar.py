"""Earnings calendar and reaction-day context utilities.

This module is intended for strategy logic that needs earnings reaction-day
classification and lookback/forward context in addition to raw earnings
events from providers.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional

try:
    from polygon import RESTClient

    POLYGON_SDK_AVAILABLE = True
except ImportError:
    POLYGON_SDK_AVAILABLE = False


class EarningsCallTime(Enum):
    """Earnings call timing relative to market hours."""

    BMO = "BMO"
    AMC = "AMC"
    DMH = "DMH"
    UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class EarningsEvent:
    """Single earnings event for calendar/reaction-day workflows."""

    symbol: str
    earnings_date: date
    call_time: EarningsCallTime
    fiscal_quarter: Optional[str] = None
    fiscal_year: Optional[int] = None

    def get_reaction_day(self) -> date:
        """Get the trading day when market reacts to these earnings."""

        if self.call_time == EarningsCallTime.AMC:
            # Simplified next-day logic. Trading-calendar adjustments are
            # intentionally left to downstream consumers if needed.
            return self.earnings_date + timedelta(days=1)
        return self.earnings_date


@dataclass(frozen=True)
class EarningsContext:
    """Earnings context for a symbol on a specific trading day."""

    is_earnings_reaction_day: bool = False
    call_time: Optional[EarningsCallTime] = None
    days_since_earnings: Optional[int] = None
    earnings_date: Optional[date] = None

    @classmethod
    def no_earnings(cls) -> EarningsContext:
        return cls(
            is_earnings_reaction_day=False,
            call_time=None,
            days_since_earnings=None,
            earnings_date=None,
        )

    def to_dict(self) -> dict:
        return {
            "is_earnings_reaction_day": self.is_earnings_reaction_day,
            "call_time": self.call_time.value if self.call_time else None,
            "days_since_earnings": self.days_since_earnings,
            "earnings_date": self.earnings_date.isoformat() if self.earnings_date else None,
        }


@dataclass
class EarningsCalendar:
    """Calendar of earnings events indexed by symbol."""

    events: dict[str, list[EarningsEvent]] = field(default_factory=dict)
    _cache_path: Optional[Path] = None

    def add_event(self, event: EarningsEvent) -> None:
        if event.symbol not in self.events:
            self.events[event.symbol] = []
        self.events[event.symbol].append(event)
        self.events[event.symbol].sort(key=lambda e: e.earnings_date)

    def get_context(self, symbol: str, trading_date: date) -> EarningsContext:
        if symbol not in self.events:
            return EarningsContext.no_earnings()

        events = self.events[symbol]
        for event in events:
            reaction_day = event.get_reaction_day()
            if reaction_day == trading_date:
                return EarningsContext(
                    is_earnings_reaction_day=True,
                    call_time=event.call_time,
                    days_since_earnings=0,
                    earnings_date=event.earnings_date,
                )

        past_events = [e for e in events if e.get_reaction_day() < trading_date]
        if past_events:
            most_recent = past_events[-1]
            days_since = (trading_date - most_recent.get_reaction_day()).days
            return EarningsContext(
                is_earnings_reaction_day=False,
                call_time=most_recent.call_time,
                days_since_earnings=days_since,
                earnings_date=most_recent.earnings_date,
            )

        return EarningsContext.no_earnings()

    def get_reaction_days(self, symbol: str, start_date: date, end_date: date) -> list[date]:
        if symbol not in self.events:
            return []

        reaction_days = []
        for event in self.events[symbol]:
            rd = event.get_reaction_day()
            if start_date <= rd <= end_date:
                reaction_days.append(rd)
        return reaction_days

    def get_days_until_earnings(
        self, symbol: str, trading_date: date, window_days: int = 30
    ) -> int | None:
        if symbol not in self.events:
            return None

        for event in self.events[symbol]:
            rd = event.get_reaction_day()
            if rd > trading_date:
                days_until = (rd - trading_date).days
                if days_until <= window_days:
                    return days_until
                return None
        return None

    def to_dict(self) -> dict:
        return {
            symbol: [
                {
                    "earnings_date": e.earnings_date.isoformat(),
                    "call_time": e.call_time.value,
                    "fiscal_quarter": e.fiscal_quarter,
                    "fiscal_year": e.fiscal_year,
                }
                for e in events
            ]
            for symbol, events in self.events.items()
        }

    @classmethod
    def from_dict(cls, data: dict, cache_path: Optional[Path] = None) -> EarningsCalendar:
        calendar = cls(_cache_path=cache_path)
        for symbol, events in data.items():
            for e in events:
                calendar.add_event(
                    EarningsEvent(
                        symbol=symbol,
                        earnings_date=date.fromisoformat(e["earnings_date"]),
                        call_time=EarningsCallTime(e["call_time"]),
                        fiscal_quarter=e.get("fiscal_quarter"),
                        fiscal_year=e.get("fiscal_year"),
                    )
                )
        return calendar

    def save(self, path: Optional[Path] = None) -> None:
        save_path = path or self._cache_path
        if save_path is None:
            raise ValueError("No path specified and no cache path set")

        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, indent=2)

    @classmethod
    def load(cls, path: Path) -> EarningsCalendar:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return cls.from_dict(data, cache_path=path)


class EarningsFetcher:
    """Fetch and cache earnings calendar data from Polygon."""

    DEFAULT_CACHE_PATH = Path("data/reference/earnings_calendar.json")

    def __init__(self, api_key: Optional[str] = None) -> None:
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise ValueError(
                "Polygon API key required. Set POLYGON_API_KEY environment variable "
                "or pass api_key parameter."
            )

        if POLYGON_SDK_AVAILABLE:
            self.client = RESTClient(self.api_key)
        else:
            import requests

            self.client = None
            self.session = requests.Session()
            self.base_url = "https://api.polygon.io"

    def fetch_earnings(
        self,
        symbols: list[str],
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> EarningsCalendar:
        if start_date is None:
            start_date = date.today() - timedelta(days=730)
        if end_date is None:
            end_date = date.today() + timedelta(days=90)

        calendar = EarningsCalendar()
        for symbol in symbols:
            events = self._fetch_symbol_earnings(symbol, start_date, end_date)
            for event in events:
                calendar.add_event(event)
        return calendar

    def _fetch_symbol_earnings(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[EarningsEvent]:
        events: list[EarningsEvent] = []
        try:
            if POLYGON_SDK_AVAILABLE and self.client is not None:
                events = self._fetch_via_sdk(symbol, start_date, end_date)
            else:
                events = self._fetch_via_rest(symbol, start_date, end_date)
        except Exception:
            pass
        return events

    def _fetch_via_sdk(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[EarningsEvent]:
        events: list[EarningsEvent] = []
        try:
            financials = self.client.vx.list_stock_financials(
                ticker=symbol.upper(),
                filing_date_gte=start_date.isoformat(),
                filing_date_lte=end_date.isoformat(),
                limit=20,
            )

            for fin in financials:
                if hasattr(fin, "filing_date") and fin.filing_date:
                    filing_date = date.fromisoformat(fin.filing_date)
                    call_time = EarningsCallTime.AMC
                    fiscal_q = getattr(fin, "fiscal_period", None)
                    fiscal_y = getattr(fin, "fiscal_year", None)
                    events.append(
                        EarningsEvent(
                            symbol=symbol.upper(),
                            earnings_date=filing_date,
                            call_time=call_time,
                            fiscal_quarter=fiscal_q,
                            fiscal_year=fiscal_y,
                        )
                    )
        except Exception:
            pass
        return events

    def _fetch_via_rest(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
    ) -> list[EarningsEvent]:
        import time

        events: list[EarningsEvent] = []
        url = f"{self.base_url}/vX/reference/financials"
        params = {
            "apiKey": self.api_key,
            "ticker": symbol.upper(),
            "filing_date.gte": start_date.isoformat(),
            "filing_date.lte": end_date.isoformat(),
            "limit": 20,
        }

        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            if "results" in data:
                for result in data["results"]:
                    filing_date_str = result.get("filing_date")
                    if filing_date_str:
                        filing_date = date.fromisoformat(filing_date_str)
                        events.append(
                            EarningsEvent(
                                symbol=symbol.upper(),
                                earnings_date=filing_date,
                                call_time=EarningsCallTime.AMC,
                                fiscal_quarter=result.get("fiscal_period"),
                                fiscal_year=result.get("fiscal_year"),
                            )
                        )
            time.sleep(0.25)
        except Exception:
            pass
        return events

    def fetch_and_cache(
        self,
        symbols: list[str],
        cache_path: Optional[Path] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> EarningsCalendar:
        path = cache_path or self.DEFAULT_CACHE_PATH
        calendar = self.fetch_earnings(symbols, start_date, end_date)
        calendar._cache_path = path
        calendar.save(path)
        return calendar


def load_earnings_calendar(path: Optional[Path] = None) -> EarningsCalendar:
    """Load earnings calendar from cache file."""

    cache_path = path or EarningsFetcher.DEFAULT_CACHE_PATH
    if not cache_path.exists():
        raise FileNotFoundError(
            f"Earnings calendar not found at {cache_path}. "
            "Fetch and cache earnings data first."
        )
    return EarningsCalendar.load(cache_path)


def get_earnings_context(
    symbol: str,
    trading_date: date,
    calendar: Optional[EarningsCalendar] = None,
) -> EarningsContext:
    """Get earnings context for a symbol on a specific date."""

    if calendar is None:
        try:
            calendar = load_earnings_calendar()
        except FileNotFoundError:
            return EarningsContext.no_earnings()
    return calendar.get_context(symbol, trading_date)
