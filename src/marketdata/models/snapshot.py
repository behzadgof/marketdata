"""Snapshot data model — combines quote with bar context."""

from __future__ import annotations

from dataclasses import dataclass

from marketdata.models.bar import Bar
from marketdata.models.quote import Quote


@dataclass(frozen=True)
class Snapshot:
    """Point-in-time snapshot combining quote and bar data.

    Attributes:
        symbol: Ticker symbol.
        quote: Current bid/ask quote.
        minute_bar: Latest completed 1-min bar.
        daily_bar: Today's running daily bar.
        prev_daily_bar: Previous trading day's daily bar.
        change: Dollar change from previous close.
        change_pct: Percent change from previous close.
    """

    symbol: str
    quote: Quote
    minute_bar: Bar | None = None
    daily_bar: Bar | None = None
    prev_daily_bar: Bar | None = None
    change: float | None = None
    change_pct: float | None = None
