"""Tests for the MockProvider — contract tests for the provider interface."""

from datetime import date, datetime, timezone

from marketdata.models.bar import Bar
from marketdata.models.earnings import EarningsEvent
from marketdata.models.quote import Quote
from marketdata.models.ticker_info import TickerInfo
from marketdata.providers.mock import MockProvider


class TestMockProviderCapabilities:
    def test_supports_all(self, mock_provider):
        caps = mock_provider.capabilities()
        assert "bars" in caps
        assert "quotes" in caps
        assert "snapshots" in caps
        assert "ticker_info" in caps
        assert "earnings" in caps
        assert "dividends" in caps
        assert "corporate_actions" in caps
        assert "calendar" in caps


class TestMockProviderBars:
    def test_auto_generates_bars(self, mock_provider):
        bars = mock_provider.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        assert len(bars) > 0
        assert all(isinstance(b, Bar) for b in bars)

    def test_bars_have_vwap(self, mock_provider):
        bars = mock_provider.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        assert all(b.vwap is not None for b in bars)

    def test_bars_have_num_trades(self, mock_provider):
        bars = mock_provider.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        assert all(b.num_trades is not None for b in bars)

    def test_preset_bars(self, mock_provider, sample_bars):
        mock_provider.set_bars("AAPL", sample_bars)
        result = mock_provider.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        assert len(result) == len(sample_bars)

    def test_skips_weekends(self, mock_provider):
        # 2024-01-13 is Saturday, 2024-01-14 is Sunday
        bars = mock_provider.get_bars("AAPL", date(2024, 1, 13), date(2024, 1, 14))
        assert len(bars) == 0


class TestMockProviderQuotes:
    def test_default_quote(self, mock_provider):
        quote = mock_provider.get_quote("AAPL")
        assert isinstance(quote, Quote)
        assert quote.symbol == "AAPL"
        assert quote.bid_price > 0
        assert quote.ask_price > quote.bid_price

    def test_preset_quote(self, mock_provider, sample_quote):
        mock_provider.set_quote("AAPL", sample_quote)
        result = mock_provider.get_quote("AAPL")
        assert result.bid_price == sample_quote.bid_price


class TestMockProviderTickerInfo:
    def test_default_info(self, mock_provider):
        info = mock_provider.get_ticker_info("AAPL")
        assert isinstance(info, TickerInfo)
        assert info.symbol == "AAPL"
        assert info.type == "CS"

    def test_preset_info(self, mock_provider):
        custom = TickerInfo(symbol="AAPL", name="Apple Inc.", sector="Technology")
        mock_provider.set_ticker_info("AAPL", custom)
        result = mock_provider.get_ticker_info("AAPL")
        assert result.sector == "Technology"


class TestMockProviderCalendar:
    def test_trading_dates(self, mock_provider):
        # Mon 2024-01-15 through Fri 2024-01-19 = 5 weekdays
        dates = mock_provider.get_trading_dates(date(2024, 1, 15), date(2024, 1, 19))
        assert len(dates) == 5

    def test_excludes_weekends(self, mock_provider):
        # Include a weekend
        dates = mock_provider.get_trading_dates(date(2024, 1, 12), date(2024, 1, 14))
        assert len(dates) == 1  # Only Friday the 12th


class TestMockProviderEarnings:
    def test_empty_by_default(self, mock_provider):
        assert mock_provider.get_earnings("AAPL") == []

    def test_preset_earnings(self, mock_provider):
        events = [
            EarningsEvent(symbol="AAPL", report_date=date(2024, 1, 25)),
        ]
        mock_provider.set_earnings("AAPL", events)
        result = mock_provider.get_earnings("AAPL")
        assert len(result) == 1
        assert result[0].report_date == date(2024, 1, 25)
