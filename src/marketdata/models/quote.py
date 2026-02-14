"""Quote (bid/ask/last) data model."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Quote:
    """Real-time quote with bid/ask and optional last trade.

    Attributes:
        symbol: Ticker symbol.
        timestamp: Quote timestamp.
        bid_price: Best bid price.
        bid_size: Size at best bid.
        ask_price: Best ask price.
        ask_size: Size at best ask.
        last_price: Last trade price.
        last_size: Last trade size.
    """

    symbol: str
    timestamp: datetime
    bid_price: float
    bid_size: float
    ask_price: float
    ask_size: float
    last_price: float | None = None
    last_size: float | None = None

    @property
    def spread(self) -> float:
        """Bid-ask spread in dollars."""
        return self.ask_price - self.bid_price

    @property
    def mid_price(self) -> float:
        """Midpoint price."""
        return (self.bid_price + self.ask_price) / 2
