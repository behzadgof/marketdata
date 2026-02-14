"""Earnings event data model."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class EarningsEvent:
    """Earnings report event.

    Attributes:
        symbol: Ticker symbol.
        report_date: Date of the earnings report.
        fiscal_quarter: Fiscal quarter (1-4).
        fiscal_year: Fiscal year.
        call_time: When the call occurs (BMO, AMC, DMH, UNKNOWN).
        status: Confirmation status (confirmed, projected).
        eps_estimate: Consensus EPS estimate.
        eps_actual: Reported EPS.
        revenue_estimate: Consensus revenue estimate.
        revenue_actual: Reported revenue.
    """

    symbol: str
    report_date: date
    fiscal_quarter: int | None = None
    fiscal_year: int | None = None
    call_time: str | None = None
    status: str | None = None
    eps_estimate: float | None = None
    eps_actual: float | None = None
    revenue_estimate: float | None = None
    revenue_actual: float | None = None
