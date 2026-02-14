"""Dividend event data model."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class DividendEvent:
    """Dividend distribution event.

    Attributes:
        symbol: Ticker symbol.
        ex_date: Ex-dividend date.
        amount: Dividend amount per share.
        record_date: Record date.
        pay_date: Payment date.
        declaration_date: Declaration date.
        dividend_type: Type (regular, special).
        frequency: Payment frequency (1=annual, 4=quarterly, 12=monthly).
        currency: Currency code.
    """

    symbol: str
    ex_date: date
    amount: float
    record_date: date | None = None
    pay_date: date | None = None
    declaration_date: date | None = None
    dividend_type: str = "regular"
    frequency: int | None = None
    currency: str = "USD"
