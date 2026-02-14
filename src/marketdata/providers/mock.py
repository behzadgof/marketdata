"""Mock provider for testing and CI — no API keys required."""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from marketdata.models.bar import Bar
from marketdata.models.corporate_action import CorporateAction
from marketdata.models.dividend import DividendEvent
from marketdata.models.earnings import EarningsEvent
from marketdata.models.quote import Quote
from marketdata.models.snapshot import Snapshot
from marketdata.models.ticker_info import TickerInfo
from marketdata.providers.base import BaseMarketDataProvider


class MockProvider(BaseMarketDataProvider):
    """In-memory provider that returns configurable static data.

    Use ``set_bars``, ``set_quote``, etc. to pre-load data, or leave
    defaults for auto-generated synthetic data.
    """

    def __init__(self) -> None:
        self._bars: dict[str, list[Bar]] = {}
        self._quotes: dict[str, Quote] = {}
        self._snapshots: dict[str, Snapshot] = {}
        self._ticker_info: dict[str, TickerInfo] = {}
        self._earnings: dict[str, list[EarningsEvent]] = {}
        self._dividends: dict[str, list[DividendEvent]] = {}
        self._corporate_actions: dict[str, list[CorporateAction]] = {}

    # --- Pre-load helpers ---

    def set_bars(self, symbol: str, bars: list[Bar]) -> None:
        self._bars[symbol.upper()] = bars

    def set_quote(self, symbol: str, quote: Quote) -> None:
        self._quotes[symbol.upper()] = quote

    def set_snapshot(self, symbol: str, snapshot: Snapshot) -> None:
        self._snapshots[symbol.upper()] = snapshot

    def set_ticker_info(self, symbol: str, info: TickerInfo) -> None:
        self._ticker_info[symbol.upper()] = info

    def set_earnings(self, symbol: str, events: list[EarningsEvent]) -> None:
        self._earnings[symbol.upper()] = events

    def set_dividends(self, symbol: str, events: list[DividendEvent]) -> None:
        self._dividends[symbol.upper()] = events

    def set_corporate_actions(self, symbol: str, actions: list[CorporateAction]) -> None:
        self._corporate_actions[symbol.upper()] = actions

    # --- Provider implementation ---

    def get_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        timeframe: str = "1min",
    ) -> list[Bar]:
        key = symbol.upper()
        if key in self._bars:
            return [
                b for b in self._bars[key]
                if start <= b.timestamp.date() <= end
            ]
        return self._generate_bars(symbol, start, end, timeframe)

    def get_quote(self, symbol: str) -> Quote:
        key = symbol.upper()
        if key in self._quotes:
            return self._quotes[key]
        return Quote(
            symbol=key,
            timestamp=datetime.now(timezone.utc),
            bid_price=149.99,
            bid_size=100.0,
            ask_price=150.01,
            ask_size=200.0,
            last_price=150.00,
            last_size=50.0,
        )

    def get_snapshot(self, symbol: str) -> Snapshot:
        key = symbol.upper()
        if key in self._snapshots:
            return self._snapshots[key]
        quote = self.get_quote(symbol)
        return Snapshot(symbol=key, quote=quote)

    def get_ticker_info(self, symbol: str) -> TickerInfo:
        key = symbol.upper()
        if key in self._ticker_info:
            return self._ticker_info[key]
        return TickerInfo(symbol=key, name=f"{key} Inc.", type="CS")

    def get_earnings(self, symbol: str, limit: int = 4) -> list[EarningsEvent]:
        key = symbol.upper()
        return self._earnings.get(key, [])[:limit]

    def get_dividends(self, symbol: str, limit: int = 12) -> list[DividendEvent]:
        key = symbol.upper()
        return self._dividends.get(key, [])[:limit]

    def get_corporate_actions(self, symbol: str) -> list[CorporateAction]:
        key = symbol.upper()
        return self._corporate_actions.get(key, [])

    def get_trading_dates(self, start: date, end: date) -> list[date]:
        """Return weekdays between start and end (simplified)."""
        dates: list[date] = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                dates.append(current)
            current += timedelta(days=1)
        return dates

    def capabilities(self) -> set[str]:
        return {
            "bars", "quotes", "snapshots", "ticker_info",
            "earnings", "dividends", "corporate_actions", "calendar",
        }

    # --- Synthetic data generation ---

    def _generate_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        timeframe: str,
    ) -> list[Bar]:
        """Generate synthetic bars for the date range."""
        bars: list[Bar] = []
        current_date = start
        base_price = 150.0

        while current_date <= end:
            if current_date.weekday() >= 5:
                current_date += timedelta(days=1)
                continue

            market_open = datetime.combine(
                current_date, time(9, 30), tzinfo=timezone.utc,
            )

            minutes = self._timeframe_minutes(timeframe)
            bars_per_day = 390 // minutes  # 6.5 hours of trading

            for i in range(bars_per_day):
                ts = market_open + timedelta(minutes=i * minutes)
                o = base_price + (i % 5) * 0.10
                h = o + 0.25
                l = o - 0.15
                c = o + 0.05
                bars.append(Bar(
                    timestamp=ts,
                    open=round(o, 2),
                    high=round(h, 2),
                    low=round(l, 2),
                    close=round(c, 2),
                    volume=10000.0 + i * 100,
                    vwap=round((o + h + l + c) / 4, 4),
                    num_trades=50 + i,
                ))

            current_date += timedelta(days=1)

        return bars

    @staticmethod
    def _timeframe_minutes(timeframe: str) -> int:
        mapping = {"1min": 1, "5min": 5, "15min": 15, "1hour": 60, "1day": 390}
        return mapping.get(timeframe, 1)
