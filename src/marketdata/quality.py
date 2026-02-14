"""Data quality validation for bars and quotes."""

from __future__ import annotations

from dataclasses import dataclass, field

from marketdata.models.bar import Bar
from marketdata.models.quote import Quote


@dataclass
class ValidationCheck:
    """Single validation check result."""

    name: str
    passed: bool
    message: str = ""


@dataclass
class ValidationResult:
    """Aggregate validation result."""

    checks: list[ValidationCheck] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def failed_checks(self) -> list[ValidationCheck]:
        return [c for c in self.checks if not c.passed]


def validate_bars(bars: list[Bar]) -> ValidationResult:
    """Run all quality checks on a list of bars.

    Checks:
        1. Not empty
        2. No null OHLCV
        3. Price sanity (no >10% single-bar moves)
        4. Volume sanity (non-negative)
        5. Timestamp ordering (chronological)
        6. Gap detection (>5 min gaps flagged)
        7. OHLC consistency (high >= low, high >= open/close)
    """
    result = ValidationResult()

    # 1. Not empty
    if not bars:
        result.checks.append(ValidationCheck("not_empty", False, "No bars provided"))
        return result
    result.checks.append(ValidationCheck("not_empty", True, f"{len(bars)} bars"))

    # 2. No null OHLCV — frozen dataclass fields are always set, so just
    #    check for NaN/inf via math
    import math

    nan_count = 0
    for b in bars:
        for val in (b.open, b.high, b.low, b.close, b.volume):
            if math.isnan(val) or math.isinf(val):
                nan_count += 1
    if nan_count:
        result.checks.append(ValidationCheck("no_nulls", False, f"{nan_count} NaN/Inf values"))
    else:
        result.checks.append(ValidationCheck("no_nulls", True))

    # 3. Price sanity — no >10% single-bar close-to-close move
    extreme = 0
    for i in range(1, len(bars)):
        prev_close = bars[i - 1].close
        if prev_close > 0:
            pct = abs(bars[i].close - prev_close) / prev_close
            if pct > 0.10:
                extreme += 1
    if extreme:
        result.checks.append(
            ValidationCheck("price_sanity", False, f"{extreme} bars with >10% move")
        )
    else:
        result.checks.append(ValidationCheck("price_sanity", True))

    # 4. Volume sanity — non-negative
    neg_vol = sum(1 for b in bars if b.volume < 0)
    if neg_vol:
        result.checks.append(
            ValidationCheck("volume_sanity", False, f"{neg_vol} bars with negative volume")
        )
    else:
        result.checks.append(ValidationCheck("volume_sanity", True))

    # 5. Timestamp ordering
    out_of_order = 0
    for i in range(1, len(bars)):
        if bars[i].timestamp <= bars[i - 1].timestamp:
            out_of_order += 1
    if out_of_order:
        result.checks.append(
            ValidationCheck("timestamp_order", False, f"{out_of_order} out of order")
        )
    else:
        result.checks.append(ValidationCheck("timestamp_order", True))

    # 6. Gap detection — >5 min gaps (exclude overnight)
    from datetime import timedelta

    large_gaps = 0
    for i in range(1, len(bars)):
        diff = bars[i].timestamp - bars[i - 1].timestamp
        if diff > timedelta(minutes=5):
            # Only flag if same day (overnight gaps are normal)
            if bars[i].timestamp.date() == bars[i - 1].timestamp.date():
                large_gaps += 1
    if large_gaps > 10:
        result.checks.append(
            ValidationCheck("gap_detection", False, f"{large_gaps} intraday gaps >5 min")
        )
    else:
        result.checks.append(ValidationCheck("gap_detection", True))

    # 7. OHLC consistency
    inconsistent = 0
    for b in bars:
        if b.high < b.low:
            inconsistent += 1
        elif b.high < b.open or b.high < b.close:
            inconsistent += 1
        elif b.low > b.open or b.low > b.close:
            inconsistent += 1
    if inconsistent:
        result.checks.append(
            ValidationCheck("ohlc_consistency", False, f"{inconsistent} bars with H<L or H<O/C")
        )
    else:
        result.checks.append(ValidationCheck("ohlc_consistency", True))

    return result


def validate_quote(quote: Quote) -> bool:
    """Basic quote sanity check.

    Returns True if bid > 0, ask > 0, ask >= bid, and spread is reasonable.
    """
    if quote.bid_price <= 0 or quote.ask_price <= 0:
        return False
    if quote.ask_price < quote.bid_price:
        return False
    # Spread sanity — more than 10% of mid is suspicious
    mid = (quote.bid_price + quote.ask_price) / 2
    if mid > 0 and quote.spread / mid > 0.10:
        return False
    return True
