"""Tests for data quality validation."""

import math
from datetime import datetime, timedelta, timezone

from marketdata.models.bar import Bar
from marketdata.models.quote import Quote
from marketdata.quality import validate_bars, validate_quote


def _make_bar(ts: datetime, close: float = 150.0, **kwargs) -> Bar:
    defaults = dict(
        timestamp=ts, open=150.0, high=151.0, low=149.0,
        close=close, volume=10000.0,
    )
    defaults.update(kwargs)
    return Bar(**defaults)


class TestValidateBars:
    def test_empty(self):
        result = validate_bars([])
        assert not result.passed
        assert result.failed_checks[0].name == "not_empty"

    def test_valid(self, sample_bars):
        result = validate_bars(sample_bars)
        assert result.passed

    def test_nan_detected(self):
        bars = [
            _make_bar(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc), close=float("nan")),
        ]
        result = validate_bars(bars)
        no_nulls = next(c for c in result.checks if c.name == "no_nulls")
        assert not no_nulls.passed

    def test_extreme_price_move(self):
        base = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        bars = [
            _make_bar(base, close=100.0),
            _make_bar(base + timedelta(minutes=1), close=115.0),  # 15% jump
        ]
        result = validate_bars(bars)
        price_check = next(c for c in result.checks if c.name == "price_sanity")
        assert not price_check.passed

    def test_negative_volume(self):
        bars = [
            _make_bar(datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc), volume=-100.0),
        ]
        result = validate_bars(bars)
        vol_check = next(c for c in result.checks if c.name == "volume_sanity")
        assert not vol_check.passed

    def test_out_of_order(self):
        base = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        bars = [
            _make_bar(base + timedelta(minutes=1)),
            _make_bar(base),  # earlier timestamp
        ]
        result = validate_bars(bars)
        order_check = next(c for c in result.checks if c.name == "timestamp_order")
        assert not order_check.passed

    def test_ohlc_inconsistency(self):
        bar = Bar(
            timestamp=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc),
            open=150.0, high=149.0, low=151.0,  # high < low!
            close=150.0, volume=10000.0,
        )
        result = validate_bars([bar])
        ohlc_check = next(c for c in result.checks if c.name == "ohlc_consistency")
        assert not ohlc_check.passed

    def test_intraday_gap(self):
        base = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
        bars = []
        for i in range(15):
            # Create bars with 10-min gaps (> 5 min threshold, same day)
            bars.append(_make_bar(base + timedelta(minutes=i * 10)))
        result = validate_bars(bars)
        gap_check = next(c for c in result.checks if c.name == "gap_detection")
        assert not gap_check.passed


class TestValidateQuote:
    def test_valid(self, sample_quote):
        assert validate_quote(sample_quote)

    def test_zero_bid(self):
        q = Quote(
            symbol="X", timestamp=datetime.now(timezone.utc),
            bid_price=0, bid_size=100, ask_price=50, ask_size=100,
        )
        assert not validate_quote(q)

    def test_inverted_spread(self):
        q = Quote(
            symbol="X", timestamp=datetime.now(timezone.utc),
            bid_price=150.0, bid_size=100,
            ask_price=140.0, ask_size=100,  # ask < bid
        )
        assert not validate_quote(q)

    def test_excessive_spread(self):
        q = Quote(
            symbol="X", timestamp=datetime.now(timezone.utc),
            bid_price=100.0, bid_size=100,
            ask_price=120.0, ask_size=100,  # 20% spread
        )
        assert not validate_quote(q)
