"""Alpaca data provider (data API only, not broker).

Provides bars, quotes, snapshots, and calendar.
Install the optional dependency:
    pip install marketdata[alpaca]
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any

from marketdata.errors import MarketDataError, MarketDataErrorCode
from marketdata.models.bar import Bar
from marketdata.models.quote import Quote
from marketdata.models.snapshot import Snapshot
from marketdata.providers.base import BaseMarketDataProvider

try:
    from alpaca.data.historical import StockHistoricalDataClient
    from alpaca.data.requests import (
        StockBarsRequest,
        StockLatestQuoteRequest,
        StockSnapshotRequest,
    )
    from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
    _ALPACA_AVAILABLE = True
except ImportError:
    _ALPACA_AVAILABLE = False


class AlpacaProvider(BaseMarketDataProvider):
    """Fetch market data from Alpaca Data API.

    Capabilities: bars, quotes, snapshots, calendar.
    """

    def __init__(
        self,
        api_key: str | None = None,
        api_secret: str | None = None,
    ) -> None:
        if not _ALPACA_AVAILABLE:
            raise MarketDataError(
                "alpaca-py is not installed. Run: pip install marketdata[alpaca]",
                code=MarketDataErrorCode.PROVIDER_ERROR,
            )

        self.api_key = api_key or os.getenv("ALPACA_API_KEY")
        self.api_secret = api_secret or os.getenv("ALPACA_SECRET_KEY")

        if not self.api_key or not self.api_secret:
            raise MarketDataError(
                "Alpaca credentials required. Set ALPACA_API_KEY and ALPACA_SECRET_KEY.",
                code=MarketDataErrorCode.AUTH_FAILED,
            )

        self.client = StockHistoricalDataClient(self.api_key, self.api_secret)

    def capabilities(self) -> set[str]:
        return {"bars", "quotes", "snapshots", "calendar"}

    _TF_MAP: dict[str, Any] = {}

    @classmethod
    def _get_tf_map(cls) -> dict[str, Any]:
        if not cls._TF_MAP and _ALPACA_AVAILABLE:
            cls._TF_MAP = {
                "1min": TimeFrame(1, TimeFrameUnit.Minute),
                "5min": TimeFrame(5, TimeFrameUnit.Minute),
                "15min": TimeFrame(15, TimeFrameUnit.Minute),
                "1hour": TimeFrame(1, TimeFrameUnit.Hour),
                "1day": TimeFrame(1, TimeFrameUnit.Day),
            }
        return cls._TF_MAP

    def get_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        timeframe: str = "1min",
    ) -> list[Bar]:
        tf_map = self._get_tf_map()
        if timeframe not in tf_map:
            raise MarketDataError(
                f"Invalid timeframe: {timeframe}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
            )

        try:
            request = StockBarsRequest(
                symbol_or_symbols=symbol.upper(),
                timeframe=tf_map[timeframe],
                start=datetime.combine(start, datetime.min.time(), tzinfo=timezone.utc),
                end=datetime.combine(end, datetime.max.time().replace(microsecond=0), tzinfo=timezone.utc),
            )
            barset = self.client.get_stock_bars(request)
            bars: list[Bar] = []
            for b in barset[symbol.upper()]:
                bars.append(Bar(
                    timestamp=b.timestamp,
                    open=float(b.open),
                    high=float(b.high),
                    low=float(b.low),
                    close=float(b.close),
                    volume=float(b.volume),
                    vwap=float(b.vwap) if hasattr(b, "vwap") and b.vwap else None,
                    num_trades=int(b.trade_count) if hasattr(b, "trade_count") and b.trade_count else None,
                ))
            return bars
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Alpaca get_bars failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def get_quote(self, symbol: str) -> Quote:
        try:
            request = StockLatestQuoteRequest(symbol_or_symbols=symbol.upper())
            result = self.client.get_stock_latest_quote(request)
            q = result[symbol.upper()]
            return Quote(
                symbol=symbol.upper(),
                timestamp=q.timestamp if hasattr(q, "timestamp") else datetime.now(timezone.utc),
                bid_price=float(q.bid_price),
                bid_size=float(q.bid_size),
                ask_price=float(q.ask_price),
                ask_size=float(q.ask_size),
            )
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Alpaca get_quote failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def get_snapshot(self, symbol: str) -> Snapshot:
        try:
            request = StockSnapshotRequest(symbol_or_symbols=symbol.upper())
            result = self.client.get_stock_snapshot(request)
            snap = result[symbol.upper()]
            quote = Quote(
                symbol=symbol.upper(),
                timestamp=datetime.now(timezone.utc),
                bid_price=float(snap.latest_quote.bid_price),
                bid_size=float(snap.latest_quote.bid_size),
                ask_price=float(snap.latest_quote.ask_price),
                ask_size=float(snap.latest_quote.ask_size),
                last_price=float(snap.latest_trade.price) if hasattr(snap, "latest_trade") else None,
                last_size=float(snap.latest_trade.size) if hasattr(snap, "latest_trade") else None,
            )
            minute_bar = None
            if hasattr(snap, "minute_bar") and snap.minute_bar:
                mb = snap.minute_bar
                minute_bar = Bar(
                    timestamp=mb.timestamp,
                    open=float(mb.open),
                    high=float(mb.high),
                    low=float(mb.low),
                    close=float(mb.close),
                    volume=float(mb.volume),
                    vwap=float(mb.vwap) if hasattr(mb, "vwap") and mb.vwap else None,
                    num_trades=int(mb.trade_count) if hasattr(mb, "trade_count") and mb.trade_count else None,
                )
            daily_bar = None
            if hasattr(snap, "daily_bar") and snap.daily_bar:
                db = snap.daily_bar
                daily_bar = Bar(
                    timestamp=db.timestamp,
                    open=float(db.open),
                    high=float(db.high),
                    low=float(db.low),
                    close=float(db.close),
                    volume=float(db.volume),
                    vwap=float(db.vwap) if hasattr(db, "vwap") and db.vwap else None,
                )
            prev_bar = None
            if hasattr(snap, "previous_daily_bar") and snap.previous_daily_bar:
                pb = snap.previous_daily_bar
                prev_bar = Bar(
                    timestamp=pb.timestamp,
                    open=float(pb.open),
                    high=float(pb.high),
                    low=float(pb.low),
                    close=float(pb.close),
                    volume=float(pb.volume),
                )
            return Snapshot(
                symbol=symbol.upper(),
                quote=quote,
                minute_bar=minute_bar,
                daily_bar=daily_bar,
                prev_daily_bar=prev_bar,
            )
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Alpaca get_snapshot failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def get_trading_dates(self, start: date, end: date) -> list[date]:
        from marketdata.calendar import get_trading_dates
        return get_trading_dates(start, end)
