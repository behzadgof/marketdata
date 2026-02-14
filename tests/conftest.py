"""Shared fixtures for marketdata tests."""

from __future__ import annotations

import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pytest

# Ensure src/ is on the path for editable-style imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from marketdata.models.bar import Bar
from marketdata.models.quote import Quote
from marketdata.providers.mock import MockProvider


@pytest.fixture
def mock_provider() -> MockProvider:
    return MockProvider()


@pytest.fixture
def sample_bars() -> list[Bar]:
    """5 contiguous 1-min bars."""
    base = datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc)
    bars = []
    for i in range(5):
        ts = base + timedelta(minutes=i)
        bars.append(Bar(
            timestamp=ts,
            open=150.0 + i * 0.1,
            high=150.5 + i * 0.1,
            low=149.5 + i * 0.1,
            close=150.2 + i * 0.1,
            volume=10000.0 + i * 500,
            vwap=150.1 + i * 0.1,
            num_trades=100 + i * 10,
        ))
    return bars


@pytest.fixture
def sample_quote() -> Quote:
    return Quote(
        symbol="AAPL",
        timestamp=datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc),
        bid_price=149.99,
        bid_size=100.0,
        ask_price=150.01,
        ask_size=200.0,
        last_price=150.00,
        last_size=50.0,
    )
