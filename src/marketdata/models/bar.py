"""Bar (OHLCV) data model."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Bar:
    """Single price bar (OHLCV + optional vwap and trade count).

    Attributes:
        timestamp: Bar timestamp (start of period).
        open: Opening price.
        high: High price.
        low: Low price.
        close: Closing price.
        volume: Trading volume.
        vwap: Volume-weighted average price (provider-supplied).
        num_trades: Number of transactions in this bar.
    """

    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    vwap: float | None = None
    num_trades: int | None = None
