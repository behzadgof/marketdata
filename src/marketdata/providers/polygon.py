"""Polygon.io data provider.

Supports both the official ``polygon-api-client`` SDK and a direct
REST fallback using ``requests``.

Install the optional dependency:
    pip install marketdata[polygon]
"""

from __future__ import annotations

import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from marketdata.errors import MarketDataError, MarketDataErrorCode
from marketdata.models.bar import Bar
from marketdata.models.dividend import DividendEvent
from marketdata.models.earnings import EarningsEvent
from marketdata.models.quote import Quote
from marketdata.models.snapshot import Snapshot
from marketdata.models.ticker_info import TickerInfo
from marketdata.providers.base import BaseMarketDataProvider

# Fix broken CURL_CA_BUNDLE env var
_curl_ca = os.environ.get("CURL_CA_BUNDLE", "")
if _curl_ca and not Path(_curl_ca).exists():
    del os.environ["CURL_CA_BUNDLE"]

try:
    from polygon import RESTClient
    _SDK_AVAILABLE = True
except ImportError:
    _SDK_AVAILABLE = False


class PolygonProvider(BaseMarketDataProvider):
    """Fetch market data from Polygon.io API.

    Capabilities: bars, quotes, snapshots, ticker_info, earnings,
    dividends, calendar.
    """

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key or os.getenv("POLYGON_API_KEY")
        if not self.api_key:
            raise MarketDataError(
                "Polygon API key required. Set POLYGON_API_KEY env var or pass api_key.",
                code=MarketDataErrorCode.AUTH_FAILED,
            )

        if _SDK_AVAILABLE:
            self.client: Any = RESTClient(self.api_key)
        else:
            import requests as _req

            self.client = None
            self.session = _req.Session()
            self.base_url = "https://api.polygon.io"
            try:
                import certifi
                self.session.verify = certifi.where()
            except ImportError:
                pass

    def capabilities(self) -> set[str]:
        return {
            "bars", "quotes", "snapshots", "ticker_info",
            "earnings", "dividends", "calendar",
        }

    # ------------------------------------------------------------------ bars

    _TF_MAP: dict[str, tuple[int, str]] = {
        "1min": (1, "minute"),
        "5min": (5, "minute"),
        "15min": (15, "minute"),
        "1hour": (1, "hour"),
        "1day": (1, "day"),
    }

    def get_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        timeframe: str = "1min",
    ) -> list[Bar]:
        if timeframe not in self._TF_MAP:
            raise MarketDataError(
                f"Invalid timeframe: {timeframe}. Valid: {list(self._TF_MAP)}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
            )
        mult, span = self._TF_MAP[timeframe]

        try:
            if _SDK_AVAILABLE and self.client is not None:
                return self._bars_sdk(symbol, start, end, mult, span)
            return self._bars_rest(symbol, start, end, mult, span)
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Polygon get_bars failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def _bars_sdk(
        self, symbol: str, start: date, end: date, mult: int, span: str,
    ) -> list[Bar]:
        aggs = self.client.get_aggs(
            ticker=symbol.upper(),
            multiplier=mult,
            timespan=span,
            from_=start.isoformat(),
            to=end.isoformat(),
            adjusted=True,
            sort="asc",
            limit=50000,
        )
        return [self._agg_to_bar(a) for a in aggs]

    def _bars_rest(
        self, symbol: str, start: date, end: date, mult: int, span: str,
    ) -> list[Bar]:
        bars: list[Bar] = []
        url: str | None = (
            f"{self.base_url}/v2/aggs/ticker/{symbol.upper()}"
            f"/range/{mult}/{span}/{start}/{end}"
        )
        params: dict[str, Any] = {
            "apiKey": self.api_key,
            "adjusted": "true",
            "sort": "asc",
            "limit": 50000,
        }

        while url:
            resp = self.session.get(url, params=params)
            self._check_response(resp)
            data = resp.json()

            for r in data.get("results", []):
                bars.append(Bar(
                    timestamp=datetime.fromtimestamp(r["t"] / 1000, tz=timezone.utc),
                    open=float(r["o"]),
                    high=float(r["h"]),
                    low=float(r["l"]),
                    close=float(r["c"]),
                    volume=float(r["v"]),
                    vwap=float(r["vw"]) if "vw" in r else None,
                    num_trades=int(r["n"]) if "n" in r else None,
                ))

            next_url = data.get("next_url")
            if next_url:
                url = next_url
                params = {"apiKey": self.api_key}
                time.sleep(0.25)
            else:
                url = None

        return bars

    @staticmethod
    def _agg_to_bar(agg: Any) -> Bar:
        return Bar(
            timestamp=datetime.fromtimestamp(agg.timestamp / 1000, tz=timezone.utc),
            open=float(agg.open),
            high=float(agg.high),
            low=float(agg.low),
            close=float(agg.close),
            volume=float(agg.volume),
            vwap=float(agg.vwap) if agg.vwap else None,
            num_trades=int(agg.transactions) if agg.transactions else None,
        )

    # --------------------------------------------------------------- quotes

    def get_quote(self, symbol: str) -> Quote:
        try:
            if _SDK_AVAILABLE and self.client is not None:
                return self._quote_sdk(symbol)
            return self._quote_rest(symbol)
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Polygon get_quote failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def _quote_sdk(self, symbol: str) -> Quote:
        snap = self.client.get_snapshot_ticker("stocks", symbol.upper())
        return self._snap_to_quote(symbol, snap)

    def _quote_rest(self, symbol: str) -> Quote:
        url = f"{self.base_url}/v2/snapshot/locale/us/markets/stocks/tickers/{symbol.upper()}"
        resp = self.session.get(url, params={"apiKey": self.api_key})
        self._check_response(resp)
        data = resp.json().get("ticker", {})
        now = datetime.now(timezone.utc)
        lq = data.get("lastQuote", {})
        lt = data.get("lastTrade", {})
        return Quote(
            symbol=symbol.upper(),
            timestamp=now,
            bid_price=float(lq.get("p", 0)),
            bid_size=float(lq.get("s", 0)),
            ask_price=float(lq.get("P", 0)),
            ask_size=float(lq.get("S", 0)),
            last_price=float(lt.get("p", 0)) if lt.get("p") else None,
            last_size=float(lt.get("s", 0)) if lt.get("s") else None,
        )

    @staticmethod
    def _snap_to_quote(symbol: str, snap: Any) -> Quote:
        now = datetime.now(timezone.utc)
        lq = getattr(snap, "last_quote", None)
        lt = getattr(snap, "last_trade", None)
        return Quote(
            symbol=symbol.upper(),
            timestamp=now,
            bid_price=float(getattr(lq, "p", 0) or 0) if lq else 0.0,
            bid_size=float(getattr(lq, "s", 0) or 0) if lq else 0.0,
            ask_price=float(getattr(lq, "P", 0) or 0) if lq else 0.0,
            ask_size=float(getattr(lq, "S", 0) or 0) if lq else 0.0,
            last_price=float(lt.p) if lt and hasattr(lt, "p") else None,
            last_size=float(lt.s) if lt and hasattr(lt, "s") else None,
        )

    # ------------------------------------------------------------ snapshots

    def get_snapshot(self, symbol: str) -> Snapshot:
        try:
            if _SDK_AVAILABLE and self.client is not None:
                return self._snapshot_sdk(symbol)
            return self._snapshot_rest(symbol)
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Polygon get_snapshot failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def _snapshot_sdk(self, symbol: str) -> Snapshot:
        snap = self.client.get_snapshot_ticker("stocks", symbol.upper())
        quote = self._snap_to_quote(symbol, snap)
        return Snapshot(
            symbol=symbol.upper(),
            quote=quote,
            change=float(snap.todays_change) if hasattr(snap, "todays_change") and snap.todays_change else None,
            change_pct=float(snap.todays_change_percent) if hasattr(snap, "todays_change_percent") and snap.todays_change_percent else None,
        )

    def _snapshot_rest(self, symbol: str) -> Snapshot:
        url = f"{self.base_url}/v2/snapshot/locale/us/markets/stocks/tickers/{symbol.upper()}"
        resp = self.session.get(url, params={"apiKey": self.api_key})
        self._check_response(resp)
        data = resp.json().get("ticker", {})
        quote = self._quote_rest(symbol)
        return Snapshot(
            symbol=symbol.upper(),
            quote=quote,
            change=float(data.get("todaysChange", 0)) if data.get("todaysChange") else None,
            change_pct=float(data.get("todaysChangePerc", 0)) if data.get("todaysChangePerc") else None,
        )

    # ---------------------------------------------------------- ticker info

    def get_ticker_info(self, symbol: str) -> TickerInfo:
        try:
            if _SDK_AVAILABLE and self.client is not None:
                return self._ticker_info_sdk(symbol)
            return self._ticker_info_rest(symbol)
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Polygon get_ticker_info failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def _ticker_info_sdk(self, symbol: str) -> TickerInfo:
        det = self.client.get_ticker_details(symbol.upper())
        return TickerInfo(
            symbol=symbol.upper(),
            name=getattr(det, "name", symbol.upper()),
            type=getattr(det, "type", "CS") or "CS",
            exchange=getattr(det, "primary_exchange", None),
            cik=str(det.cik) if getattr(det, "cik", None) else None,
            composite_figi=getattr(det, "composite_figi", None),
            share_class_figi=getattr(det, "share_class_figi", None),
            sector=getattr(det, "sic_description", None),
            market_cap=float(det.market_cap) if getattr(det, "market_cap", None) else None,
            shares_outstanding=float(det.share_class_shares_outstanding) if getattr(det, "share_class_shares_outstanding", None) else None,
        )

    def _ticker_info_rest(self, symbol: str) -> TickerInfo:
        url = f"{self.base_url}/v3/reference/tickers/{symbol.upper()}"
        resp = self.session.get(url, params={"apiKey": self.api_key})
        self._check_response(resp)
        r = resp.json().get("results", {})
        return TickerInfo(
            symbol=symbol.upper(),
            name=r.get("name", symbol.upper()),
            type=r.get("type", "CS") or "CS",
            exchange=r.get("primary_exchange"),
            cik=str(r["cik"]) if r.get("cik") else None,
            composite_figi=r.get("composite_figi"),
            share_class_figi=r.get("share_class_figi"),
            sector=r.get("sic_description"),
            market_cap=float(r["market_cap"]) if r.get("market_cap") else None,
            shares_outstanding=float(r["share_class_shares_outstanding"]) if r.get("share_class_shares_outstanding") else None,
        )

    # ------------------------------------------------------------- earnings

    def get_earnings(self, symbol: str, limit: int = 4) -> list[EarningsEvent]:
        try:
            if _SDK_AVAILABLE and self.client is not None:
                return self._earnings_sdk(symbol, limit)
            return self._earnings_rest(symbol, limit)
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Polygon get_earnings failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def _earnings_sdk(self, symbol: str, limit: int) -> list[EarningsEvent]:
        events: list[EarningsEvent] = []
        try:
            financials = self.client.vx.list_stock_financials(
                ticker=symbol.upper(), limit=limit,
            )
            for fin in financials:
                if hasattr(fin, "filing_date") and fin.filing_date:
                    events.append(EarningsEvent(
                        symbol=symbol.upper(),
                        report_date=date.fromisoformat(fin.filing_date),
                        fiscal_quarter=int(fin.fiscal_period[1]) if getattr(fin, "fiscal_period", None) and fin.fiscal_period.startswith("Q") else None,
                        fiscal_year=int(fin.fiscal_year) if getattr(fin, "fiscal_year", None) else None,
                        call_time="AMC",
                    ))
        except Exception:
            pass
        return events[:limit]

    def _earnings_rest(self, symbol: str, limit: int) -> list[EarningsEvent]:
        events: list[EarningsEvent] = []
        url = f"{self.base_url}/vX/reference/financials"
        params: dict[str, Any] = {
            "apiKey": self.api_key,
            "ticker": symbol.upper(),
            "limit": limit,
        }
        try:
            resp = self.session.get(url, params=params)
            self._check_response(resp)
            for r in resp.json().get("results", []):
                fd = r.get("filing_date")
                if fd:
                    fp = r.get("fiscal_period", "")
                    events.append(EarningsEvent(
                        symbol=symbol.upper(),
                        report_date=date.fromisoformat(fd),
                        fiscal_quarter=int(fp[1]) if fp.startswith("Q") else None,
                        fiscal_year=int(r["fiscal_year"]) if r.get("fiscal_year") else None,
                        call_time="AMC",
                    ))
        except MarketDataError:
            raise
        except Exception:
            pass
        return events[:limit]

    # ------------------------------------------------------------ dividends

    def get_dividends(self, symbol: str, limit: int = 12) -> list[DividendEvent]:
        try:
            if _SDK_AVAILABLE and self.client is not None:
                return self._dividends_sdk(symbol, limit)
            return self._dividends_rest(symbol, limit)
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"Polygon get_dividends failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def _dividends_sdk(self, symbol: str, limit: int) -> list[DividendEvent]:
        events: list[DividendEvent] = []
        try:
            divs = self.client.list_dividends(ticker=symbol.upper(), limit=limit)
            for d in divs:
                events.append(DividendEvent(
                    symbol=symbol.upper(),
                    ex_date=date.fromisoformat(d.ex_dividend_date) if hasattr(d, "ex_dividend_date") and d.ex_dividend_date else date.today(),
                    amount=float(d.cash_amount) if hasattr(d, "cash_amount") else 0.0,
                    record_date=date.fromisoformat(d.record_date) if getattr(d, "record_date", None) else None,
                    pay_date=date.fromisoformat(d.pay_date) if getattr(d, "pay_date", None) else None,
                    declaration_date=date.fromisoformat(d.declaration_date) if getattr(d, "declaration_date", None) else None,
                    dividend_type=getattr(d, "dividend_type", "regular") or "regular",
                    frequency=int(d.frequency) if getattr(d, "frequency", None) else None,
                ))
        except Exception:
            pass
        return events[:limit]

    def _dividends_rest(self, symbol: str, limit: int) -> list[DividendEvent]:
        events: list[DividendEvent] = []
        url = f"{self.base_url}/v3/reference/dividends"
        params: dict[str, Any] = {
            "apiKey": self.api_key,
            "ticker": symbol.upper(),
            "limit": limit,
        }
        try:
            resp = self.session.get(url, params=params)
            self._check_response(resp)
            for r in resp.json().get("results", []):
                events.append(DividendEvent(
                    symbol=symbol.upper(),
                    ex_date=date.fromisoformat(r["ex_dividend_date"]) if r.get("ex_dividend_date") else date.today(),
                    amount=float(r.get("cash_amount", 0)),
                    record_date=date.fromisoformat(r["record_date"]) if r.get("record_date") else None,
                    pay_date=date.fromisoformat(r["pay_date"]) if r.get("pay_date") else None,
                    declaration_date=date.fromisoformat(r["declaration_date"]) if r.get("declaration_date") else None,
                    dividend_type=r.get("dividend_type", "regular") or "regular",
                    frequency=int(r["frequency"]) if r.get("frequency") else None,
                ))
        except MarketDataError:
            raise
        except Exception:
            pass
        return events[:limit]

    # ------------------------------------------------------------- calendar

    def get_trading_dates(self, start: date, end: date) -> list[date]:
        try:
            if _SDK_AVAILABLE and self.client is not None:
                return self._calendar_sdk(start, end)
            return self._calendar_rest(start, end)
        except Exception:
            # Fallback: weekdays only
            from marketdata.calendar import get_trading_dates
            return get_trading_dates(start, end)

    def _calendar_sdk(self, start: date, end: date) -> list[date]:
        from marketdata.calendar import get_trading_dates
        return get_trading_dates(start, end)

    def _calendar_rest(self, start: date, end: date) -> list[date]:
        from marketdata.calendar import get_trading_dates
        return get_trading_dates(start, end)

    # ------------------------------------------------------------ internals

    def _check_response(self, resp: Any) -> None:
        if resp.status_code == 429:
            raise MarketDataError(
                "Polygon rate limited",
                code=MarketDataErrorCode.RATE_LIMITED,
                retryable=True,
            )
        if resp.status_code == 403:
            raise MarketDataError(
                "Polygon authentication failed",
                code=MarketDataErrorCode.AUTH_FAILED,
            )
        if resp.status_code == 404:
            raise MarketDataError(
                "Symbol not found on Polygon",
                code=MarketDataErrorCode.NOT_FOUND,
            )
        resp.raise_for_status()
