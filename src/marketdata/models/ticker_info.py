"""Ticker reference data model."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TickerInfo:
    """Reference data for a ticker, merged across providers.

    Attributes:
        symbol: Ticker symbol.
        name: Full legal name.
        type: Security type (CS=Common Stock, ETF, ADR, etc.).
        exchange: Primary exchange.
        cusip: CUSIP identifier (IB/Finnhub).
        isin: ISIN identifier (IB/Finnhub).
        cik: SEC CIK number (Polygon/SEC EDGAR).
        composite_figi: OpenFIGI composite (Polygon).
        share_class_figi: OpenFIGI share class (Polygon).
        sector: GICS sector.
        industry: GICS industry.
        subcategory: More granular industry (IB).
        market_cap: Market capitalization in USD.
        shares_outstanding: Total shares outstanding.
        trading_hours: Liquid trading hours string (IB).
        min_tick: Minimum price increment (IB).
        shortable: Whether easy to borrow (IB).
    """

    symbol: str
    name: str
    type: str = "CS"
    exchange: str | None = None
    cusip: str | None = None
    isin: str | None = None
    cik: str | None = None
    composite_figi: str | None = None
    share_class_figi: str | None = None
    sector: str | None = None
    industry: str | None = None
    subcategory: str | None = None
    market_cap: float | None = None
    shares_outstanding: float | None = None
    trading_hours: str | None = None
    min_tick: float | None = None
    shortable: bool | None = None
