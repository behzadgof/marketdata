"""Abstract base class for market data providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date

from marketdata.models.bar import Bar
from marketdata.models.corporate_action import CorporateAction
from marketdata.models.dividend import DividendEvent
from marketdata.models.earnings import EarningsEvent
from marketdata.models.quote import Quote
from marketdata.models.snapshot import Snapshot
from marketdata.models.ticker_info import TickerInfo


class BaseMarketDataProvider(ABC):
    """Abstract base for all market data providers.

    Subclasses must implement ``get_bars``. All other methods default to
    ``NotImplementedError`` — providers implement only the endpoints they
    support and advertise them via ``capabilities()``.
    """

    # --- Historical bars (required) ---

    @abstractmethod
    def get_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        timeframe: str = "1min",
    ) -> list[Bar]:
        """Fetch historical OHLCV bars.

        Args:
            symbol: Ticker symbol.
            start: Start date (inclusive).
            end: End date (inclusive).
            timeframe: Bar size — "1min", "5min", "15min", "1hour", "1day".

        Returns:
            List of Bar objects ordered by timestamp ascending.
        """
        ...

    # --- Real-time ---

    def get_quote(self, symbol: str) -> Quote:
        """Get the current bid/ask quote for a symbol."""
        raise NotImplementedError

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        """Get quotes for multiple symbols (default: serial calls)."""
        return [self.get_quote(s) for s in symbols]

    def get_snapshot(self, symbol: str) -> Snapshot:
        """Get a point-in-time snapshot (quote + bar context)."""
        raise NotImplementedError

    def get_snapshots(self, symbols: list[str]) -> list[Snapshot]:
        """Get snapshots for multiple symbols (default: serial calls)."""
        return [self.get_snapshot(s) for s in symbols]

    # --- Reference data ---

    def get_ticker_info(self, symbol: str) -> TickerInfo:
        """Get reference data for a ticker."""
        raise NotImplementedError

    def get_earnings(self, symbol: str, limit: int = 4) -> list[EarningsEvent]:
        """Get recent earnings events."""
        raise NotImplementedError

    def get_dividends(self, symbol: str, limit: int = 12) -> list[DividendEvent]:
        """Get recent dividend events."""
        raise NotImplementedError

    def get_corporate_actions(self, symbol: str) -> list[CorporateAction]:
        """Get corporate actions (splits, mergers, etc.)."""
        raise NotImplementedError

    # --- Calendar ---

    def get_trading_dates(self, start: date, end: date) -> list[date]:
        """Get trading dates within a range (excluding weekends/holidays)."""
        raise NotImplementedError

    # --- Capabilities ---

    def capabilities(self) -> set[str]:
        """Return the set of supported features.

        Possible values: ``bars``, ``quotes``, ``snapshots``,
        ``ticker_info``, ``earnings``, ``dividends``,
        ``corporate_actions``, ``calendar``.
        """
        return {"bars"}
