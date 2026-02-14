"""Tests for cache backends (Parquet and Memory)."""

import time
from datetime import date
from pathlib import Path

import pytest

from marketdata.cache import MemoryCache, NoCache, ParquetCache


class TestNoCache:
    def test_always_misses(self, sample_bars):
        cache = NoCache()
        assert cache.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15), "1min") is None
        cache.store_bars("AAPL", sample_bars, "1min", date(2024, 1, 15), date(2024, 1, 15))
        assert cache.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15), "1min") is None
        assert not cache.has_data("AAPL", "1min", date(2024, 1, 15), date(2024, 1, 15))


class TestParquetCache:
    @pytest.fixture
    def cache(self, tmp_path):
        return ParquetCache(tmp_path / "cache")

    def test_store_and_retrieve(self, cache, sample_bars):
        start = date(2024, 1, 15)
        end = date(2024, 1, 15)
        cache.store_bars("AAPL", sample_bars, "1min", start, end)
        assert cache.has_data("AAPL", "1min", start, end)

        result = cache.get_bars("AAPL", start, end, "1min")
        assert result is not None
        assert len(result) == len(sample_bars)
        assert result[0].open == sample_bars[0].open
        assert result[0].vwap == sample_bars[0].vwap
        assert result[0].num_trades == sample_bars[0].num_trades

    def test_miss(self, cache):
        assert cache.get_bars("AAPL", date(2024, 1, 15), date(2024, 1, 15), "1min") is None
        assert not cache.has_data("AAPL", "1min", date(2024, 1, 15), date(2024, 1, 15))

    def test_clear_symbol(self, cache, sample_bars):
        cache.store_bars("AAPL", sample_bars, "1min", date(2024, 1, 15), date(2024, 1, 15))
        cache.store_bars("MSFT", sample_bars, "1min", date(2024, 1, 15), date(2024, 1, 15))
        cache.clear("AAPL")
        assert not cache.has_data("AAPL", "1min", date(2024, 1, 15), date(2024, 1, 15))
        assert cache.has_data("MSFT", "1min", date(2024, 1, 15), date(2024, 1, 15))

    def test_clear_all(self, cache, sample_bars):
        cache.store_bars("AAPL", sample_bars, "1min", date(2024, 1, 15), date(2024, 1, 15))
        cache.store_bars("MSFT", sample_bars, "1min", date(2024, 1, 15), date(2024, 1, 15))
        cache.clear_all()
        assert not cache.has_data("AAPL", "1min", date(2024, 1, 15), date(2024, 1, 15))
        assert not cache.has_data("MSFT", "1min", date(2024, 1, 15), date(2024, 1, 15))

    def test_empty_bars_not_stored(self, cache):
        cache.store_bars("AAPL", [], "1min", date(2024, 1, 15), date(2024, 1, 15))
        assert not cache.has_data("AAPL", "1min", date(2024, 1, 15), date(2024, 1, 15))


class TestMemoryCache:
    def test_store_and_retrieve(self, sample_bars):
        cache = MemoryCache(ttl_seconds=60)
        start = date(2024, 1, 15)
        end = date(2024, 1, 15)
        cache.store_bars("AAPL", sample_bars, "1min", start, end)
        result = cache.get_bars("AAPL", start, end, "1min")
        assert result is not None
        assert len(result) == len(sample_bars)

    def test_ttl_expiry(self, sample_bars):
        cache = MemoryCache(ttl_seconds=0)  # Immediate expiry
        start = date(2024, 1, 15)
        end = date(2024, 1, 15)
        cache.store_bars("AAPL", sample_bars, "1min", start, end)
        time.sleep(0.01)  # Ensure time advances
        result = cache.get_bars("AAPL", start, end, "1min")
        assert result is None

    def test_lru_eviction(self, sample_bars):
        cache = MemoryCache(ttl_seconds=300, max_entries=2)
        start = date(2024, 1, 15)
        cache.store_bars("AAPL", sample_bars, "1min", start, start)
        cache.store_bars("MSFT", sample_bars, "1min", start, start)
        cache.store_bars("GOOG", sample_bars, "1min", start, start)
        # AAPL should be evicted (LRU)
        assert cache.get_bars("AAPL", start, start, "1min") is None
        assert cache.get_bars("MSFT", start, start, "1min") is not None
        assert cache.get_bars("GOOG", start, start, "1min") is not None

    def test_clear_symbol(self, sample_bars):
        cache = MemoryCache(ttl_seconds=60)
        start = date(2024, 1, 15)
        cache.store_bars("AAPL", sample_bars, "1min", start, start)
        cache.store_bars("MSFT", sample_bars, "1min", start, start)
        cache.clear("AAPL")
        assert cache.get_bars("AAPL", start, start, "1min") is None
        assert cache.get_bars("MSFT", start, start, "1min") is not None

    def test_clear_all(self, sample_bars):
        cache = MemoryCache(ttl_seconds=60)
        start = date(2024, 1, 15)
        cache.store_bars("AAPL", sample_bars, "1min", start, start)
        cache.clear_all()
        assert cache.get_bars("AAPL", start, start, "1min") is None
