"""Tests for MarketDataManager — fallback chain, cache, validation."""

from datetime import date, datetime, timedelta, timezone

import pytest

from marketdata.config import MarketDataConfig, MarketDataProviderType
from marketdata.errors import MarketDataError, MarketDataErrorCode
from marketdata.manager import MarketDataManager
from marketdata.models.bar import Bar
from marketdata.models.quote import Quote
from marketdata.models.ticker_info import TickerInfo
from marketdata.providers.mock import MockProvider


def _make_manager(
    *,
    cache_backend: str = "memory",
    validate: bool = False,
) -> MarketDataManager:
    config = MarketDataConfig(
        providers=[MarketDataProviderType.MOCK],
        cache_backend=cache_backend,
        validate=validate,
    )
    return MarketDataManager(config)


class TestManagerBars:
    def test_get_bars(self):
        mgr = _make_manager()
        bars = mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        assert len(bars) > 0
        assert all(isinstance(b, Bar) for b in bars)

    def test_cache_hit(self):
        mgr = _make_manager(cache_backend="memory")
        bars1 = mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        bars2 = mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        # Same data from cache
        assert len(bars1) == len(bars2)

    def test_parquet_cache(self, tmp_path):
        config = MarketDataConfig(
            providers=[MarketDataProviderType.MOCK],
            cache_backend="parquet",
            cache_dir=str(tmp_path / "cache"),
            validate=False,
        )
        mgr = MarketDataManager(config)
        bars1 = mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        bars2 = mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        assert len(bars1) == len(bars2)

    def test_no_cache(self):
        mgr = _make_manager(cache_backend="none")
        bars = mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        assert len(bars) > 0

    def test_validation_passes(self):
        mgr = _make_manager(validate=True)
        bars = mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        assert len(bars) > 0


class TestManagerFallback:
    def test_fallback_on_retryable_error(self):
        """If first provider fails with retryable error, try next."""
        config = MarketDataConfig(
            providers=[MarketDataProviderType.MOCK, MarketDataProviderType.MOCK],
            cache_backend="none",
            validate=False,
        )
        mgr = MarketDataManager(config)

        # Sabotage first provider
        original_get_bars = mgr.providers[0].get_bars
        def failing_get_bars(*args, **kwargs):
            raise MarketDataError("fail", MarketDataErrorCode.TIMEOUT, retryable=True)
        mgr.providers[0].get_bars = failing_get_bars  # type: ignore[method-assign]

        bars = mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        assert len(bars) > 0

    def test_non_retryable_raises_immediately(self):
        """Non-retryable errors skip fallback and raise."""
        config = MarketDataConfig(
            providers=[MarketDataProviderType.MOCK, MarketDataProviderType.MOCK],
            cache_backend="none",
            validate=False,
        )
        mgr = MarketDataManager(config)

        def failing_get_bars(*args, **kwargs):
            raise MarketDataError("auth fail", MarketDataErrorCode.AUTH_FAILED, retryable=False)
        mgr.providers[0].get_bars = failing_get_bars  # type: ignore[method-assign]

        with pytest.raises(MarketDataError) as exc_info:
            mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        assert exc_info.value.code == MarketDataErrorCode.AUTH_FAILED

    def test_all_providers_fail_raises(self):
        config = MarketDataConfig(
            providers=[MarketDataProviderType.MOCK],
            cache_backend="none",
            validate=False,
        )
        mgr = MarketDataManager(config)

        def failing_get_bars(*args, **kwargs):
            raise MarketDataError("timeout", MarketDataErrorCode.TIMEOUT, retryable=True)
        mgr.providers[0].get_bars = failing_get_bars  # type: ignore[method-assign]

        with pytest.raises(MarketDataError):
            mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))


class TestManagerTickerInfoMerge:
    def test_merge_across_providers(self):
        """TickerInfo fields are merged from multiple providers."""
        config = MarketDataConfig(
            providers=[MarketDataProviderType.MOCK, MarketDataProviderType.MOCK],
            cache_backend="none",
        )
        mgr = MarketDataManager(config)

        # Set up provider 1 with sector
        info1 = TickerInfo(symbol="AAPL", name="Apple Inc.", sector="Technology")
        mgr.providers[0].set_ticker_info("AAPL", info1)  # type: ignore[attr-defined]

        # Set up provider 2 with CUSIP (no sector)
        info2 = TickerInfo(symbol="AAPL", name="Apple Inc.", cusip="037833100")
        mgr.providers[1].set_ticker_info("AAPL", info2)  # type: ignore[attr-defined]

        result = mgr.get_ticker_info("AAPL")
        assert result.sector == "Technology"
        assert result.cusip == "037833100"


class TestManagerQuotes:
    def test_get_quote(self):
        mgr = _make_manager()
        quote = mgr.get_quote("AAPL")
        assert isinstance(quote, Quote)
        assert quote.symbol == "AAPL"

    def test_get_quotes(self):
        mgr = _make_manager()
        quotes = mgr.get_quotes(["AAPL", "MSFT"])
        assert len(quotes) == 2


class TestManagerCalendar:
    def test_get_trading_dates(self):
        mgr = _make_manager()
        dates = mgr.get_trading_dates(date(2024, 1, 15), date(2024, 1, 19))
        assert len(dates) == 5  # Mock uses simple weekday logic


class TestManagerCache:
    def test_clear_cache(self):
        mgr = _make_manager(cache_backend="memory")
        mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        mgr.clear_cache("AAPL")
        # Next call should miss cache and re-fetch
        bars = mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        assert len(bars) > 0

    def test_clear_all_cache(self):
        mgr = _make_manager(cache_backend="memory")
        mgr.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15))
        mgr.get_bars("MSFT", date(2024, 1, 15), date(2024, 1, 15))
        mgr.clear_all_cache()
