"""MarketDataManager — central orchestrator with cache + provider fallback."""

from __future__ import annotations

from dataclasses import fields
from datetime import date
from typing import Any

from marketdata.cache import CacheBackend, MemoryCache, NoCache, ParquetCache
from marketdata.config import MarketDataConfig
from marketdata.errors import MarketDataError, MarketDataErrorCode
from marketdata.models.bar import Bar
from marketdata.models.corporate_action import CorporateAction
from marketdata.models.dividend import DividendEvent
from marketdata.models.earnings import EarningsEvent
from marketdata.models.quote import Quote
from marketdata.models.snapshot import Snapshot
from marketdata.models.ticker_info import TickerInfo
from marketdata.providers import create_provider
from marketdata.providers.base import BaseMarketDataProvider
from marketdata.quality import validate_bars, validate_quote


class MarketDataManager:
    """Central orchestrator: cache -> provider -> validate -> fallback.

    Usage::

        from marketdata import create_manager_from_env
        mgr = create_manager_from_env()
        symbol = "YOUR_SYMBOL"
        bars = mgr.get_bars(symbol, date(2024, 1, 2), date(2024, 1, 5))
    """

    def __init__(self, config: MarketDataConfig) -> None:
        self.config = config

        # Build provider chain
        self.providers: list[BaseMarketDataProvider] = []
        for pt in config.providers:
            kwargs: dict[str, Any] = {}
            if pt.value == "polygon" and config.polygon_api_key:
                kwargs["api_key"] = config.polygon_api_key
            elif pt.value == "alpaca":
                if config.alpaca_api_key:
                    kwargs["api_key"] = config.alpaca_api_key
                if config.alpaca_api_secret:
                    kwargs["api_secret"] = config.alpaca_api_secret
            elif pt.value == "finnhub" and config.finnhub_api_key:
                kwargs["api_key"] = config.finnhub_api_key
            elif pt.value == "ib":
                kwargs["host"] = config.ib_host
                kwargs["port"] = config.ib_port
                kwargs["client_id"] = config.ib_client_id
            self.providers.append(create_provider(pt, **kwargs))

        # Build cache
        self.cache: CacheBackend
        if config.cache_backend == "parquet":
            self.cache = ParquetCache(config.cache_dir)
        elif config.cache_backend == "memory":
            self.cache = MemoryCache(ttl_seconds=config.cache_ttl_seconds)
        else:
            self.cache = NoCache()

    # ----------------------------------------------------------------- bars

    def get_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        timeframe: str = "1min",
    ) -> list[Bar]:
        """Get bars: cache -> provider chain -> validate -> store.

        Tries each provider in order. Retryable errors fall through to
        the next provider; non-retryable errors are raised immediately.
        """
        # 1. Cache hit?
        cached = self.cache.get_bars(symbol, start, end, timeframe)
        if cached is not None:
            return cached

        # 2. Try providers
        last_error: MarketDataError | None = None
        for provider in self.providers:
            if "bars" not in provider.capabilities():
                continue
            try:
                bars = provider.get_bars(symbol, start, end, timeframe)

                # 3. Quality gate
                if self.config.validate:
                    result = validate_bars(bars)
                    if not result.passed:
                        msgs = "; ".join(c.message for c in result.failed_checks)
                        raise MarketDataError(
                            f"Validation failed: {msgs}",
                            code=MarketDataErrorCode.VALIDATION_FAILED,
                            retryable=True,
                        )

                # 4. Store in cache
                self.cache.store_bars(symbol, bars, timeframe, start, end)
                return bars

            except MarketDataError as e:
                if not e.retryable:
                    raise
                last_error = e
                continue

        raise last_error or MarketDataError(
            "All providers failed",
            code=MarketDataErrorCode.NO_DATA,
        )

    # --------------------------------------------------------------- quotes

    def get_quote(self, symbol: str) -> Quote:
        """Get a quote from the first capable provider."""
        return self._first_capable("quotes", "get_quote", symbol)

    def get_quotes(self, symbols: list[str]) -> list[Quote]:
        """Get quotes for multiple symbols."""
        return [self.get_quote(s) for s in symbols]

    # ------------------------------------------------------------ snapshots

    def get_snapshot(self, symbol: str) -> Snapshot:
        return self._first_capable("snapshots", "get_snapshot", symbol)

    def get_snapshots(self, symbols: list[str]) -> list[Snapshot]:
        return [self.get_snapshot(s) for s in symbols]

    # ---------------------------------------------------------- ticker info

    def get_ticker_info(self, symbol: str) -> TickerInfo:
        """Get ticker info, merging fields across all capable providers."""
        info: dict[str, Any] = {"symbol": symbol.upper(), "name": symbol.upper()}
        for provider in self.providers:
            if "ticker_info" not in provider.capabilities():
                continue
            try:
                result = provider.get_ticker_info(symbol)
                for f in fields(TickerInfo):
                    val = getattr(result, f.name)
                    if val is not None and info.get(f.name) is None:
                        info[f.name] = val
            except Exception:
                continue
        return TickerInfo(**info)

    # ------------------------------------------------------------- earnings

    def get_earnings(self, symbol: str, limit: int = 4) -> list[EarningsEvent]:
        return self._first_capable("earnings", "get_earnings", symbol, limit=limit)

    # ------------------------------------------------------------ dividends

    def get_dividends(self, symbol: str, limit: int = 12) -> list[DividendEvent]:
        return self._first_capable("dividends", "get_dividends", symbol, limit=limit)

    # ------------------------------------------------------- corp actions

    def get_corporate_actions(self, symbol: str) -> list[CorporateAction]:
        return self._first_capable("corporate_actions", "get_corporate_actions", symbol)

    # ------------------------------------------------------------- calendar

    def get_trading_dates(self, start: date, end: date) -> list[date]:
        for provider in self.providers:
            if "calendar" in provider.capabilities():
                return provider.get_trading_dates(start, end)
        from marketdata.calendar import get_trading_dates
        return get_trading_dates(start, end)

    # --------------------------------------------------------------- cache

    def clear_cache(self, symbol: str) -> None:
        self.cache.clear(symbol)

    def clear_all_cache(self) -> None:
        self.cache.clear_all()

    # ------------------------------------------------------------ internal

    def _first_capable(self, capability: str, method: str, *args: Any, **kwargs: Any) -> Any:
        """Try providers in order for a given capability."""
        last_error: MarketDataError | None = None
        for provider in self.providers:
            if capability not in provider.capabilities():
                continue
            try:
                return getattr(provider, method)(*args, **kwargs)
            except MarketDataError as e:
                if not e.retryable:
                    raise
                last_error = e
                continue
            except NotImplementedError:
                continue

        raise last_error or MarketDataError(
            f"No provider supports '{capability}'",
            code=MarketDataErrorCode.NO_DATA,
        )

