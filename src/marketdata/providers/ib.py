"""Interactive Brokers data provider via ib_insync.

Provides bars, quotes, and rich reference data (CUSIP, ISIN, trading hours).
Requires a running TWS or IB Gateway instance.

Install the optional dependency:
    pip install marketdata[ib]
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from datetime import date, datetime, timedelta, timezone
from typing import Any

from marketdata.errors import MarketDataError, MarketDataErrorCode
from marketdata.models.bar import Bar
from marketdata.models.dividend import DividendEvent
from marketdata.models.quote import Quote
from marketdata.models.ticker_info import TickerInfo
from marketdata.providers.base import BaseMarketDataProvider

try:
    from ib_insync import IB, Stock, util
    _IB_AVAILABLE = True
except ImportError:
    _IB_AVAILABLE = False


class IBProvider(BaseMarketDataProvider):
    """Fetch market data from Interactive Brokers via ib_insync.

    Capabilities: bars, quotes, ticker_info, dividends.
    """

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        client_id: int | None = None,
    ) -> None:
        if not _IB_AVAILABLE:
            raise MarketDataError(
                "ib_insync is not installed. Run: pip install marketdata[ib]",
                code=MarketDataErrorCode.PROVIDER_ERROR,
            )

        self.host = host or os.getenv("IB_HOST", "127.0.0.1")
        self.port = port or int(os.getenv("IB_PORT", "7497"))
        self.client_id = client_id or int(os.getenv("IB_CLIENT_ID", "1"))
        self._ib: Any = None

    def _connect(self) -> Any:
        if self._ib is None or not self._ib.isConnected():
            self._ib = IB()
            try:
                self._ib.connect(self.host, self.port, clientId=self.client_id)
            except Exception as exc:
                raise MarketDataError(
                    f"Cannot connect to IB TWS/Gateway at {self.host}:{self.port}: {exc}",
                    code=MarketDataErrorCode.PROVIDER_ERROR,
                    retryable=True,
                ) from exc
        return self._ib

    def capabilities(self) -> set[str]:
        return {"bars", "quotes", "ticker_info", "dividends"}

    _TF_MAP: dict[str, str] = {
        "1min": "1 min",
        "5min": "5 mins",
        "15min": "15 mins",
        "1hour": "1 hour",
        "1day": "1 day",
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
                f"Invalid timeframe: {timeframe}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
            )

        try:
            ib = self._connect()
            contract = Stock(symbol.upper(), "SMART", "USD")
            ib.qualifyContracts(contract)

            days = (end - start).days + 1
            duration = f"{days} D"

            ib_bars = ib.reqHistoricalData(
                contract,
                endDateTime=datetime.combine(end, datetime.max.time().replace(microsecond=0)),
                durationStr=duration,
                barSizeSetting=self._TF_MAP[timeframe],
                whatToShow="TRADES",
                useRTH=True,
                formatDate=2,
            )

            bars: list[Bar] = []
            for b in ib_bars:
                ts = b.date if isinstance(b.date, datetime) else datetime.combine(b.date, datetime.min.time())
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts.date() < start:
                    continue
                bars.append(Bar(
                    timestamp=ts,
                    open=float(b.open),
                    high=float(b.high),
                    low=float(b.low),
                    close=float(b.close),
                    volume=float(b.volume),
                    num_trades=int(b.barCount) if hasattr(b, "barCount") else None,
                ))
            return bars
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"IB get_bars failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def get_quote(self, symbol: str) -> Quote:
        try:
            ib = self._connect()
            contract = Stock(symbol.upper(), "SMART", "USD")
            ib.qualifyContracts(contract)
            ticker = ib.reqMktData(contract, snapshot=True)
            ib.sleep(2)  # Wait for data
            return Quote(
                symbol=symbol.upper(),
                timestamp=datetime.now(timezone.utc),
                bid_price=float(ticker.bid) if ticker.bid and ticker.bid > 0 else 0.0,
                bid_size=float(ticker.bidSize) if ticker.bidSize else 0.0,
                ask_price=float(ticker.ask) if ticker.ask and ticker.ask > 0 else 0.0,
                ask_size=float(ticker.askSize) if ticker.askSize else 0.0,
                last_price=float(ticker.last) if ticker.last and ticker.last > 0 else None,
                last_size=float(ticker.lastSize) if ticker.lastSize else None,
            )
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"IB get_quote failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def get_ticker_info(self, symbol: str) -> TickerInfo:
        try:
            ib = self._connect()
            contract = Stock(symbol.upper(), "SMART", "USD")
            details_list = ib.reqContractDetails(contract)
            if not details_list:
                raise MarketDataError(
                    f"No contract details for {symbol}",
                    code=MarketDataErrorCode.NOT_FOUND,
                )
            det = details_list[0]
            c = det.contract
            return TickerInfo(
                symbol=symbol.upper(),
                name=det.longName or symbol.upper(),
                type=c.secType or "CS",
                exchange=c.primaryExchange or None,
                cusip=det.cusip if hasattr(det, "cusip") and det.cusip else None,
                isin=det.isin if hasattr(det, "isin") and det.isin else None,
                industry=det.industry if hasattr(det, "industry") else None,
                subcategory=det.subcategory if hasattr(det, "subcategory") else None,
                trading_hours=det.liquidHours if hasattr(det, "liquidHours") else None,
                min_tick=float(det.minTick) if hasattr(det, "minTick") and det.minTick else None,
                shortable=None,  # Requires separate API call
            )
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"IB get_ticker_info failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    # Frequency text → integer mapping for dividend frequency
    _FREQ_MAP: dict[str, int] = {
        "monthly": 12, "quarterly": 4, "semi-annual": 2,
        "semi-annually": 2, "semiannual": 2, "annual": 1,
        "annually": 1,
    }

    def get_dividends(self, symbol: str, limit: int = 12) -> list[DividendEvent]:
        try:
            ib = self._connect()
            contract = Stock(symbol.upper(), "SMART", "USD")
            ib.qualifyContracts(contract)

            # Method 1: CalendarReport XML (full history, requires Reuters subscription)
            events = self._dividends_from_calendar_report(ib, contract, symbol)
            if events:
                events.sort(key=lambda e: e.ex_date, reverse=True)
                return events[:limit]

            # Method 2: Generic tick 456 (basic next-dividend only)
            events = self._dividends_from_tick(ib, contract, symbol)
            if events:
                return events[:limit]

            return []
        except MarketDataError:
            raise
        except Exception as exc:
            raise MarketDataError(
                f"IB get_dividends failed: {exc}",
                code=MarketDataErrorCode.PROVIDER_ERROR,
                retryable=True,
            ) from exc

    def _dividends_from_calendar_report(
        self, ib: Any, contract: Any, symbol: str,
    ) -> list[DividendEvent]:
        """Parse dividend events from IB CalendarReport XML.

        The CalendarReport XML from Thomson Reuters/Refinitiv uses varying
        structures across IB versions.  This parser searches recursively for
        dividend record elements and extracts fields by matching common tag
        name patterns.
        """
        try:
            xml_data = ib.reqFundamentalData(contract, "CalendarReport")
            if not xml_data:
                return []
        except Exception:
            return []

        try:
            root = ET.fromstring(xml_data)
        except ET.ParseError:
            return []

        events: list[DividendEvent] = []

        for elem in root.iter():
            tag = self._clean_tag(elem.tag)

            # Match elements representing a single dividend record
            is_dividend_elem = tag in ("cashdividend", "dividend")
            is_event_div = (
                tag == "event"
                and elem.get("type", "").lower() == "dividend"
            )
            if not is_dividend_elem and not is_event_div:
                continue

            fields = self._extract_dividend_fields(elem)
            ex_date = self._parse_date_flexible(fields.get("ex_date"))
            if not ex_date:
                continue

            amount_str = fields.get("amount")
            if not amount_str:
                continue
            try:
                amount = float(amount_str)
            except (ValueError, TypeError):
                continue

            freq_text = (fields.get("frequency") or "").lower()
            frequency = self._FREQ_MAP.get(freq_text)

            div_type_raw = (fields.get("dividend_type") or "").lower()
            div_type = "special" if "special" in div_type_raw else "regular"

            events.append(DividendEvent(
                symbol=symbol.upper(),
                ex_date=ex_date,
                amount=amount,
                record_date=self._parse_date_flexible(fields.get("record_date")),
                pay_date=self._parse_date_flexible(fields.get("pay_date")),
                declaration_date=self._parse_date_flexible(fields.get("declaration_date")),
                dividend_type=div_type,
                frequency=frequency,
                currency=fields.get("currency", "USD"),
            ))

        return events

    def _dividends_from_tick(
        self, ib: Any, contract: Any, symbol: str,
    ) -> list[DividendEvent]:
        """Get basic dividend info via generic tick type 456.

        Returns a string on ticker.dividends in the format:
            past12Months,next12Months,nextExDate,nextAmount
        Example: "0.96,0.96,2024-08-10,0.24"
        """
        try:
            ticker = ib.reqMktData(contract, genericTickList="456")
            ib.sleep(2)
            div_str = getattr(ticker, "dividends", None)
            ib.cancelMktData(contract)

            if not div_str or not isinstance(div_str, str):
                return []

            parts = [p.strip() for p in div_str.split(",")]
            if len(parts) < 4:
                return []

            # parts: [past12mo, next12mo, nextExDate, nextAmount]
            ex_date = self._parse_date_flexible(parts[2])
            if not ex_date:
                return []

            try:
                amount = float(parts[3])
            except (ValueError, TypeError):
                return []

            return [DividendEvent(
                symbol=symbol.upper(),
                ex_date=ex_date,
                amount=amount,
                dividend_type="regular",
                currency="USD",
            )]
        except Exception:
            return []

    # ---- XML helper methods ----

    # Tag-name sets for matching child elements in CalendarReport XML.
    # IB / Thomson Reuters use varying naming across versions.
    _EX_DATE_TAGS = {"exdate", "ex_date", "eventdate"}
    _PAY_DATE_TAGS = {"paydate", "pay_date", "paymentdate"}
    _RECORD_DATE_TAGS = {"recorddate", "record_date"}
    _DECL_DATE_TAGS = {
        "declaredate", "declarationdate", "decl_date",
        "declaration_date", "decldate",
    }
    _AMOUNT_TAGS = {"amount", "dividendamount", "div_amount"}
    _TYPE_TAGS = {"dividendtype", "divtype", "dividend_type", "type"}
    _FREQ_TAGS = {"frequency", "freq"}
    _CURRENCY_TAGS = {"currency", "curr"}

    @staticmethod
    def _clean_tag(tag: str) -> str:
        """Strip XML namespace prefix and lowercase."""
        return tag.rsplit("}", 1)[-1].lower()

    def _extract_dividend_fields(self, elem: ET.Element) -> dict[str, str]:
        """Walk an element and its nested children to collect dividend fields."""
        result: dict[str, str] = {}

        # Search the element itself and one level of nested containers
        # (e.g. <Detail> or <EventBody> inside <Event>)
        containers = [elem]
        for child in elem:
            tag = self._clean_tag(child.tag)
            if tag in ("detail", "eventbody", "dividenddetail"):
                containers.append(child)

        for container in containers:
            for child in container:
                tag = self._clean_tag(child.tag)
                text = (child.text or "").strip()
                if not text:
                    continue

                if tag in self._EX_DATE_TAGS and "ex_date" not in result:
                    result["ex_date"] = text
                elif tag in self._PAY_DATE_TAGS and "pay_date" not in result:
                    result["pay_date"] = text
                elif tag in self._RECORD_DATE_TAGS and "record_date" not in result:
                    result["record_date"] = text
                elif tag in self._DECL_DATE_TAGS and "declaration_date" not in result:
                    result["declaration_date"] = text
                elif tag in self._AMOUNT_TAGS and "amount" not in result:
                    result["amount"] = text
                elif tag in self._TYPE_TAGS and "dividend_type" not in result:
                    result["dividend_type"] = text
                elif tag in self._FREQ_TAGS and "frequency" not in result:
                    result["frequency"] = text
                elif tag in self._CURRENCY_TAGS and "currency" not in result:
                    result["currency"] = text

        return result

    @staticmethod
    def _parse_date_flexible(text: str | None) -> date | None:
        """Parse a date string trying common IB/Reuters formats."""
        if not text:
            return None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y", "%Y%m%d"):
            try:
                return datetime.strptime(text.strip(), fmt).date()
            except ValueError:
                continue
        return None
