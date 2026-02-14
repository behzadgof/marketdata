"""Finnhub data provider — best standalone source for identifiers.

Provides ticker_info (CUSIP, ISIN, NAICS, GICS) and earnings.
Install the optional dependency:
    pip install marketdata[finnhub]
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any

from marketdata.errors import MarketDataError, MarketDataErrorCode
from marketdata.models.bar import Bar
from marketdata.models.earnings import EarningsEvent
from marketdata.models.ticker_info import TickerInfo
from marketdata.providers.base import BaseMarketDataProvider

try:
    import finnhub
    _FINNHUB_AVAILABLE = True
except ImportError:
    _FINNHUB_AVAILABLE = False


class FinnhubProvider(BaseMarketDataProvider):
    """Fetch reference data from Finnhub.io.

    Capabilities: ticker_info, earnings.
    Note: ``get_bars`` is implemented but Finnhub's free tier has limited
    historical data. Use Polygon or Alpaca for bars.
    """

    def __init__(self, api_key: str | None = None) -> None:
        if not _FINNHUB_AVAILABLE:
            raise MarketDataError(
                "finnhub-python is not installed. Run: pip install marketdata[finnhub]",
                code=MarketDataErrorCode.PROVIDER_ERROR,
            )

        self.api_key = api_key or os.getenv("FINNHUB_API_KEY")
        if not self.api_key:
            raise MarketDataError(
                "Finnhub API key required. Set FINNHUB_API_KEY env var or pass api_key.",
                code=MarketDataErrorCode.AUTH_FAILED,
            )

        self.client = finnhub.Client(api_key=self.api_key)

    def capabilities(self) -> set[str]:
        return {"bars", "ticker_info", "earnings"}

    def get_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        timeframe: str = "1min",
    ) -> list[Bar]:
        resolution_map = {
            "1min": "1",
            "5min": "5",
            "15min": "15",
            "1hour": "60",
            "1day": "D",
        }
        if timeframe not in resolution_map:
            raise MarketDataError(
                f"Invalid timeframe: {timeframe}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
            )

        try:
            start_ts = int(datetime.combine(start, datetime.min.time()).replace(tzinfo=timezone.utc).timestamp())
            end_ts = int(datetime.combine(end, datetime.max.time().replace(microsecond=0)).replace(tzinfo=timezone.utc).timestamp())

            data = self.client.stock_candles(
                symbol.upper(),
                resolution_map[timeframe],
                start_ts,
                end_ts,
            )

            if data.get("s") != "ok":
                return []

            bars: list[Bar] = []
            for i in range(len(data.get("t", []))):
                bars.append(Bar(
                    timestamp=datetime.fromtimestamp(data["t"][i], tz=timezone.utc),
                    open=float(data["o"][i]),
                    high=float(data["h"][i]),
                    low=float(data["l"][i]),
                    close=float(data["c"][i]),
                    volume=float(data["v"][i]),
                ))
            return bars
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Finnhub get_bars failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def get_ticker_info(self, symbol: str) -> TickerInfo:
        try:
            profile = self.client.company_profile2(symbol=symbol.upper())
            if not profile:
                raise MarketDataError(
                    f"No profile for {symbol}",
                    code=MarketDataErrorCode.NOT_FOUND,
                )
            return TickerInfo(
                symbol=symbol.upper(),
                name=profile.get("name", symbol.upper()),
                type="CS",
                exchange=profile.get("exchange"),
                cusip=profile.get("cusip"),
                isin=profile.get("isin"),
                sector=profile.get("finnhubIndustry"),
                industry=profile.get("finnhubIndustry"),
                market_cap=float(profile["marketCapitalization"]) * 1_000_000 if profile.get("marketCapitalization") else None,
                shares_outstanding=float(profile["shareOutstanding"]) * 1_000_000 if profile.get("shareOutstanding") else None,
            )
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Finnhub get_ticker_info failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def get_earnings(self, symbol: str, limit: int = 4) -> list[EarningsEvent]:
        try:
            data = self.client.company_earnings(symbol.upper(), limit=limit)
            events: list[EarningsEvent] = []
            for e in data:
                period = e.get("period")
                if not period:
                    continue
                events.append(EarningsEvent(
                    symbol=symbol.upper(),
                    report_date=date.fromisoformat(period),
                    fiscal_quarter=e.get("quarter"),
                    fiscal_year=e.get("year"),
                    eps_estimate=float(e["estimate"]) if e.get("estimate") is not None else None,
                    eps_actual=float(e["actual"]) if e.get("actual") is not None else None,
                ))
            return events[:limit]
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Finnhub get_earnings failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc
