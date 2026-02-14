"""Tests for data models."""

from datetime import date, datetime, timezone

from marketdata.models.bar import Bar
from marketdata.models.corporate_action import CorporateAction
from marketdata.models.dividend import DividendEvent
from marketdata.models.earnings import EarningsEvent
from marketdata.models.quote import Quote
from marketdata.models.snapshot import Snapshot
from marketdata.models.ticker_info import TickerInfo


class TestBar:
    def test_create(self):
        bar = Bar(
            timestamp=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc),
            open=150.0, high=151.0, low=149.0, close=150.5,
            volume=10000.0, vwap=150.3, num_trades=200,
        )
        assert bar.open == 150.0
        assert bar.vwap == 150.3
        assert bar.num_trades == 200

    def test_optional_fields_default_none(self):
        bar = Bar(
            timestamp=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc),
            open=150.0, high=151.0, low=149.0, close=150.5, volume=10000.0,
        )
        assert bar.vwap is None
        assert bar.num_trades is None

    def test_frozen(self):
        bar = Bar(
            timestamp=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc),
            open=150.0, high=151.0, low=149.0, close=150.5, volume=10000.0,
        )
        import pytest
        with pytest.raises(AttributeError):
            bar.close = 999.0  # type: ignore[misc]


class TestQuote:
    def test_spread(self, sample_quote):
        assert abs(sample_quote.spread - 0.02) < 1e-9

    def test_mid_price(self, sample_quote):
        assert abs(sample_quote.mid_price - 150.0) < 1e-9

    def test_optional_last(self):
        q = Quote(
            symbol="AAPL",
            timestamp=datetime(2024, 1, 15, tzinfo=timezone.utc),
            bid_price=100.0, bid_size=50.0,
            ask_price=100.10, ask_size=75.0,
        )
        assert q.last_price is None
        assert q.last_size is None


class TestSnapshot:
    def test_create_minimal(self, sample_quote):
        snap = Snapshot(symbol="AAPL", quote=sample_quote)
        assert snap.minute_bar is None
        assert snap.change is None

    def test_create_full(self, sample_quote, sample_bars):
        snap = Snapshot(
            symbol="AAPL",
            quote=sample_quote,
            minute_bar=sample_bars[0],
            daily_bar=sample_bars[-1],
            change=1.50,
            change_pct=1.01,
        )
        assert snap.change == 1.50
        assert snap.minute_bar.open == sample_bars[0].open


class TestTickerInfo:
    def test_defaults(self):
        info = TickerInfo(symbol="AAPL", name="Apple Inc.")
        assert info.type == "CS"
        assert info.cusip is None
        assert info.shortable is None

    def test_full(self):
        info = TickerInfo(
            symbol="AAPL", name="Apple Inc.", type="CS",
            exchange="XNAS", cusip="037833100", isin="US0378331005",
            cik="320193", composite_figi="BBG000B9XRY4",
            sector="Technology", industry="Consumer Electronics",
            market_cap=3e12, shares_outstanding=15.4e9,
        )
        assert info.cik == "320193"
        assert info.market_cap == 3e12


class TestEarningsEvent:
    def test_create(self):
        e = EarningsEvent(
            symbol="AAPL", report_date=date(2024, 1, 25),
            fiscal_quarter=1, fiscal_year=2024, call_time="AMC",
            eps_estimate=2.10, eps_actual=2.18,
        )
        assert e.fiscal_quarter == 1
        assert e.eps_actual == 2.18


class TestDividendEvent:
    def test_defaults(self):
        d = DividendEvent(
            symbol="AAPL", ex_date=date(2024, 2, 9), amount=0.24,
        )
        assert d.dividend_type == "regular"
        assert d.currency == "USD"
        assert d.frequency is None


class TestCorporateAction:
    def test_split(self):
        ca = CorporateAction(
            symbol="AAPL", action_type="split",
            effective_date=date(2020, 8, 31),
            details={"ratio": "4:1"},
        )
        assert ca.action_type == "split"
        assert ca.details["ratio"] == "4:1"
