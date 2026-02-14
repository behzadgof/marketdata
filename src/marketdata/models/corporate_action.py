"""Corporate action data model."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class CorporateAction:
    """Corporate action event (splits, mergers, spinoffs, etc.).

    Attributes:
        symbol: Ticker symbol.
        action_type: Type of action (split, reverse_split, merger, spinoff, name_change).
        ex_date: Ex-date for the action.
        effective_date: Effective date.
        details: Additional details (ratio for splits, terms for mergers, etc.).
    """

    symbol: str
    action_type: str
    ex_date: date | None = None
    effective_date: date | None = None
    details: dict | None = None
