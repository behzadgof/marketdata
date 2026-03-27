"""Comprehensive unit tests for the marketdata streaming module.

Covers:
- Symbol mapping for BinanceStreamingProvider and CoinbaseStreamingProvider
- StreamManager behavior with zero and one provider
- Provider message handling (_handle_trade, _handle_mini_ticker, _handle_ticker)
- BaseStreamingProvider callback registration, emission, and exception isolation
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from marketdata.models.bar import Bar
from marketdata.streaming.base import BaseStreamingProvider, BarCallback, QuoteCallback
from marketdata.streaming.binance import BinanceStreamingProvider
from marketdata.streaming.coinbase import CoinbaseStreamingProvider
from marketdata.streaming.manager import StreamManager


# ---------------------------------------------------------------------------
# Concrete stub for BaseStreamingProvider (abstract method implementations)
# ---------------------------------------------------------------------------

class ConcreteProvider(BaseStreamingProvider):
    """Minimal concrete subclass for testing BaseStreamingProvider behaviour."""

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    async def subscribe(self, symbols: list[str], channels: list[str]) -> None:
        for sym in symbols:
            self._subscribed_symbols.add(sym)

    async def unsubscribe(self, symbols: list[str]) -> None:
        for sym in symbols:
            self._subscribed_symbols.discard(sym)


# ===========================================================================
# 1. Symbol mapping – BinanceStreamingProvider
# ===========================================================================

class TestBinanceSymbolMapping:
    """Tests for _to_binance_symbol and _from_binance_symbol."""

    def test_to_binance_btc_usd_slash(self) -> None:
        assert BinanceStreamingProvider._to_binance_symbol("BTC/USD") == "btcusdt"

    def test_to_binance_eth_usd_slash(self) -> None:
        assert BinanceStreamingProvider._to_binance_symbol("ETH/USD") == "ethusdt"

    def test_to_binance_bare_btc(self) -> None:
        """A bare symbol with no quote currency is lowercased as-is (no USDT suffix added)."""
        # The implementation only appends T when the stripped symbol ends with USD.
        # A bare "BTC" does not end with USD, so it becomes "btc".
        assert BinanceStreamingProvider._to_binance_symbol("BTC") == "btc"

    def test_to_binance_bare_eth(self) -> None:
        """A bare ETH symbol with no quote currency becomes 'eth'."""
        assert BinanceStreamingProvider._to_binance_symbol("ETH") == "eth"

    def test_to_binance_lowercase_input(self) -> None:
        """Input is case-insensitive."""
        assert BinanceStreamingProvider._to_binance_symbol("btc/usd") == "btcusdt"

    def test_to_binance_already_usdt_no_double_suffix(self) -> None:
        """BTC/USDT should not become btcusdtt."""
        result = BinanceStreamingProvider._to_binance_symbol("BTC/USDT")
        assert result == "btcusdt"

    def test_from_binance_btcusdt(self) -> None:
        assert BinanceStreamingProvider._from_binance_symbol("BTCUSDT") == "BTC/USD"

    def test_from_binance_ethusdt(self) -> None:
        assert BinanceStreamingProvider._from_binance_symbol("ETHUSDT") == "ETH/USD"

    def test_from_binance_lowercase_input(self) -> None:
        """Input is uppercased before parsing."""
        assert BinanceStreamingProvider._from_binance_symbol("btcusdt") == "BTC/USD"

    def test_from_binance_unknown_suffix_passthrough(self) -> None:
        """Symbols that don't end with USDT are returned uppercased as-is."""
        result = BinanceStreamingProvider._from_binance_symbol("BTCETH")
        assert result == "BTCETH"

    def test_roundtrip_btc(self) -> None:
        binance = BinanceStreamingProvider._to_binance_symbol("BTC/USD")
        restored = BinanceStreamingProvider._from_binance_symbol(binance.upper())
        assert restored == "BTC/USD"

    def test_roundtrip_eth(self) -> None:
        binance = BinanceStreamingProvider._to_binance_symbol("ETH/USD")
        restored = BinanceStreamingProvider._from_binance_symbol(binance.upper())
        assert restored == "ETH/USD"


# ===========================================================================
# 2. Symbol mapping – CoinbaseStreamingProvider
# ===========================================================================

class TestCoinbaseSymbolMapping:
    """Tests for _to_coinbase_symbol and _from_coinbase_symbol."""

    def test_to_coinbase_btc_usd(self) -> None:
        assert CoinbaseStreamingProvider._to_coinbase_symbol("BTC/USD") == "BTC-USD"

    def test_to_coinbase_eth_usd(self) -> None:
        assert CoinbaseStreamingProvider._to_coinbase_symbol("ETH/USD") == "ETH-USD"

    def test_to_coinbase_bare_symbol_appends_usd(self) -> None:
        assert CoinbaseStreamingProvider._to_coinbase_symbol("ETH") == "ETH-USD"

    def test_to_coinbase_bare_btc(self) -> None:
        assert CoinbaseStreamingProvider._to_coinbase_symbol("BTC") == "BTC-USD"

    def test_to_coinbase_lowercase_input(self) -> None:
        """Input is uppercased."""
        assert CoinbaseStreamingProvider._to_coinbase_symbol("btc/usd") == "BTC-USD"

    def test_to_coinbase_sol_usd(self) -> None:
        assert CoinbaseStreamingProvider._to_coinbase_symbol("SOL/USD") == "SOL-USD"

    def test_from_coinbase_btc_usd(self) -> None:
        assert CoinbaseStreamingProvider._from_coinbase_symbol("BTC-USD") == "BTC/USD"

    def test_from_coinbase_eth_usd(self) -> None:
        assert CoinbaseStreamingProvider._from_coinbase_symbol("ETH-USD") == "ETH/USD"

    def test_from_coinbase_lowercase_input(self) -> None:
        assert CoinbaseStreamingProvider._from_coinbase_symbol("btc-usd") == "BTC/USD"

    def test_roundtrip_btc(self) -> None:
        product_id = CoinbaseStreamingProvider._to_coinbase_symbol("BTC/USD")
        restored = CoinbaseStreamingProvider._from_coinbase_symbol(product_id)
        assert restored == "BTC/USD"

    def test_roundtrip_eth(self) -> None:
        product_id = CoinbaseStreamingProvider._to_coinbase_symbol("ETH/USD")
        restored = CoinbaseStreamingProvider._from_coinbase_symbol(product_id)
        assert restored == "ETH/USD"


# ===========================================================================
# 3. StreamManager – no network calls required
# ===========================================================================

class TestStreamManagerEmpty:
    """StreamManager with no providers."""

    def test_has_providers_false(self) -> None:
        manager = StreamManager([])
        assert manager.has_providers is False

    def test_is_connected_false(self) -> None:
        manager = StreamManager([])
        assert manager.is_connected is False

    def test_on_quote_with_no_providers_does_not_raise(self) -> None:
        manager = StreamManager([])
        callback: QuoteCallback = MagicMock()
        manager.on_quote(callback)  # should not raise

    def test_on_bar_with_no_providers_does_not_raise(self) -> None:
        manager = StreamManager([])
        callback: BarCallback = MagicMock()
        manager.on_bar(callback)  # should not raise


class TestStreamManagerWithProvider:
    """StreamManager with a single mock provider."""

    def _make_manager(self) -> tuple[StreamManager, ConcreteProvider]:
        provider = ConcreteProvider()
        manager = StreamManager([provider])
        return manager, provider

    def test_has_providers_true(self) -> None:
        manager, _ = self._make_manager()
        assert manager.has_providers is True

    def test_is_connected_false_before_connect(self) -> None:
        manager, _ = self._make_manager()
        assert manager.is_connected is False

    def test_is_connected_true_when_provider_connected(self) -> None:
        manager, provider = self._make_manager()
        provider._connected = True
        assert manager.is_connected is True

    def test_is_connected_false_when_all_disconnected(self) -> None:
        manager, provider = self._make_manager()
        provider._connected = False
        assert manager.is_connected is False

    def test_on_quote_registers_callback_on_provider(self) -> None:
        manager, provider = self._make_manager()
        callback: QuoteCallback = MagicMock()
        manager.on_quote(callback)
        assert callback in provider._quote_callbacks

    def test_on_bar_registers_callback_on_provider(self) -> None:
        manager, provider = self._make_manager()
        callback: BarCallback = MagicMock()
        manager.on_bar(callback)
        assert callback in provider._bar_callbacks

    def test_on_quote_propagates_to_all_providers(self) -> None:
        p1 = ConcreteProvider()
        p2 = ConcreteProvider()
        manager = StreamManager([p1, p2])
        callback: QuoteCallback = MagicMock()
        manager.on_quote(callback)
        assert callback in p1._quote_callbacks
        assert callback in p2._quote_callbacks

    def test_on_bar_propagates_to_all_providers(self) -> None:
        p1 = ConcreteProvider()
        p2 = ConcreteProvider()
        manager = StreamManager([p1, p2])
        callback: BarCallback = MagicMock()
        manager.on_bar(callback)
        assert callback in p1._bar_callbacks
        assert callback in p2._bar_callbacks

    def test_is_connected_true_if_at_least_one_connected(self) -> None:
        p1 = ConcreteProvider()
        p2 = ConcreteProvider()
        p1._connected = False
        p2._connected = True
        manager = StreamManager([p1, p2])
        assert manager.is_connected is True


# ===========================================================================
# 4. BinanceStreamingProvider – message handling
# ===========================================================================

class TestBinanceHandleTrade:
    """Tests for BinanceStreamingProvider._handle_trade."""

    def _make_provider(self) -> BinanceStreamingProvider:
        return BinanceStreamingProvider(url="wss://fake")

    def _make_trade_msg(
        self,
        symbol: str = "BTCUSDT",
        price: str = "45000.50",
        qty: str = "0.125",
        trade_time_ms: int = 1_700_000_000_000,
    ) -> dict[str, Any]:
        return {
            "e": "trade",
            "s": symbol,
            "p": price,
            "q": qty,
            "T": trade_time_ms,
        }

    def test_callback_invoked_on_trade(self) -> None:
        provider = self._make_provider()
        callback: QuoteCallback = MagicMock()
        provider.on_quote(callback)

        msg = self._make_trade_msg()
        provider._handle_trade(msg)

        callback.assert_called_once()

    def test_correct_symbol_passed_to_callback(self) -> None:
        provider = self._make_provider()
        received: list[str] = []
        provider.on_quote(lambda sym, p, s, t: received.append(sym))

        provider._handle_trade(self._make_trade_msg(symbol="ETHUSDT"))

        assert received == ["ETH/USD"]

    def test_correct_price_passed_to_callback(self) -> None:
        provider = self._make_provider()
        received: list[float] = []
        provider.on_quote(lambda sym, price, s, t: received.append(price))

        provider._handle_trade(self._make_trade_msg(price="30000.99"))

        assert received == [30000.99]

    def test_correct_size_passed_to_callback(self) -> None:
        provider = self._make_provider()
        received: list[float] = []
        provider.on_quote(lambda sym, p, size, t: received.append(size))

        provider._handle_trade(self._make_trade_msg(qty="2.5"))

        assert received == [2.5]

    def test_correct_timestamp_passed_to_callback(self) -> None:
        provider = self._make_provider()
        received: list[datetime] = []
        provider.on_quote(lambda sym, p, s, ts: received.append(ts))

        ts_ms = 1_700_000_000_000
        provider._handle_trade(self._make_trade_msg(trade_time_ms=ts_ms))

        expected = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        assert received == [expected]

    def test_timestamp_is_utc_aware(self) -> None:
        provider = self._make_provider()
        received: list[datetime] = []
        provider.on_quote(lambda sym, p, s, ts: received.append(ts))

        provider._handle_trade(self._make_trade_msg())

        assert received[0].tzinfo is not None
        assert received[0].tzinfo == timezone.utc

    def test_multiple_callbacks_all_invoked(self) -> None:
        provider = self._make_provider()
        cb1: QuoteCallback = MagicMock()
        cb2: QuoteCallback = MagicMock()
        provider.on_quote(cb1)
        provider.on_quote(cb2)

        provider._handle_trade(self._make_trade_msg())

        cb1.assert_called_once()
        cb2.assert_called_once()

    def test_btc_usd_trade_symbol_mapping(self) -> None:
        provider = self._make_provider()
        received: list[str] = []
        provider.on_quote(lambda sym, p, s, t: received.append(sym))

        provider._handle_trade(self._make_trade_msg(symbol="BTCUSDT"))

        assert received == ["BTC/USD"]


class TestBinanceHandleMiniTicker:
    """Tests for BinanceStreamingProvider._handle_mini_ticker."""

    def _make_provider(self) -> BinanceStreamingProvider:
        return BinanceStreamingProvider(url="wss://fake")

    def _make_mini_ticker_msg(
        self,
        symbol: str = "BTCUSDT",
        close: str = "46000.00",
        volume: str = "1500.0",
        event_time_ms: int = 1_700_001_000_000,
    ) -> dict[str, Any]:
        return {
            "e": "24hrMiniTicker",
            "s": symbol,
            "c": close,
            "o": "45000.00",
            "h": "47000.00",
            "l": "44000.00",
            "v": volume,
            "q": "67500000.00",
            "E": event_time_ms,
        }

    def test_callback_invoked_on_mini_ticker(self) -> None:
        provider = self._make_provider()
        callback: QuoteCallback = MagicMock()
        provider.on_quote(callback)

        provider._handle_mini_ticker(self._make_mini_ticker_msg())

        callback.assert_called_once()

    def test_correct_symbol_from_mini_ticker(self) -> None:
        provider = self._make_provider()
        received: list[str] = []
        provider.on_quote(lambda sym, p, s, t: received.append(sym))

        provider._handle_mini_ticker(self._make_mini_ticker_msg(symbol="ETHUSDT"))

        assert received == ["ETH/USD"]

    def test_price_is_close_price(self) -> None:
        """Mini ticker uses the close/last price field 'c'."""
        provider = self._make_provider()
        received: list[float] = []
        provider.on_quote(lambda sym, price, s, t: received.append(price))

        provider._handle_mini_ticker(self._make_mini_ticker_msg(close="46500.75"))

        assert received == [46500.75]

    def test_size_is_volume(self) -> None:
        """Mini ticker passes volume 'v' as the size argument."""
        provider = self._make_provider()
        received: list[float] = []
        provider.on_quote(lambda sym, p, size, t: received.append(size))

        provider._handle_mini_ticker(self._make_mini_ticker_msg(volume="2000.0"))

        assert received == [2000.0]

    def test_timestamp_from_event_time(self) -> None:
        provider = self._make_provider()
        received: list[datetime] = []
        provider.on_quote(lambda sym, p, s, ts: received.append(ts))

        ts_ms = 1_700_001_000_000
        provider._handle_mini_ticker(self._make_mini_ticker_msg(event_time_ms=ts_ms))

        expected = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        assert received == [expected]

    def test_timestamp_is_utc_aware(self) -> None:
        provider = self._make_provider()
        received: list[datetime] = []
        provider.on_quote(lambda sym, p, s, ts: received.append(ts))

        provider._handle_mini_ticker(self._make_mini_ticker_msg())

        assert received[0].tzinfo == timezone.utc

    def test_btc_usd_mini_ticker_symbol_mapping(self) -> None:
        provider = self._make_provider()
        received: list[str] = []
        provider.on_quote(lambda sym, p, s, t: received.append(sym))

        provider._handle_mini_ticker(self._make_mini_ticker_msg(symbol="BTCUSDT"))

        assert received == ["BTC/USD"]

    def test_multiple_callbacks_all_invoked(self) -> None:
        provider = self._make_provider()
        cb1: QuoteCallback = MagicMock()
        cb2: QuoteCallback = MagicMock()
        provider.on_quote(cb1)
        provider.on_quote(cb2)

        provider._handle_mini_ticker(self._make_mini_ticker_msg())

        cb1.assert_called_once()
        cb2.assert_called_once()


# ===========================================================================
# 5. CoinbaseStreamingProvider – message handling
# ===========================================================================

class TestCoinbaseHandleTicker:
    """Tests for CoinbaseStreamingProvider._handle_ticker."""

    def _make_provider(self) -> CoinbaseStreamingProvider:
        return CoinbaseStreamingProvider(url="wss://fake")

    def _make_ticker_msg(
        self,
        product_id: str = "BTC-USD",
        price: str = "45000.00",
        last_size: str = "0.5",
        time: str = "2023-11-14T20:00:00Z",
    ) -> dict[str, Any]:
        return {
            "type": "ticker",
            "product_id": product_id,
            "price": price,
            "last_size": last_size,
            "time": time,
        }

    def test_callback_invoked_on_ticker(self) -> None:
        provider = self._make_provider()
        callback: QuoteCallback = MagicMock()
        provider.on_quote(callback)

        provider._handle_ticker(self._make_ticker_msg())

        callback.assert_called_once()

    def test_correct_symbol_from_ticker(self) -> None:
        provider = self._make_provider()
        received: list[str] = []
        provider.on_quote(lambda sym, p, s, t: received.append(sym))

        provider._handle_ticker(self._make_ticker_msg(product_id="BTC-USD"))

        assert received == ["BTC/USD"]

    def test_eth_usd_ticker_symbol_mapping(self) -> None:
        provider = self._make_provider()
        received: list[str] = []
        provider.on_quote(lambda sym, p, s, t: received.append(sym))

        provider._handle_ticker(self._make_ticker_msg(product_id="ETH-USD"))

        assert received == ["ETH/USD"]

    def test_correct_price_from_ticker(self) -> None:
        provider = self._make_provider()
        received: list[float] = []
        provider.on_quote(lambda sym, price, s, t: received.append(price))

        provider._handle_ticker(self._make_ticker_msg(price="50000.25"))

        assert received == [50000.25]

    def test_correct_size_from_ticker(self) -> None:
        provider = self._make_provider()
        received: list[float] = []
        provider.on_quote(lambda sym, p, size, t: received.append(size))

        provider._handle_ticker(self._make_ticker_msg(last_size="1.234"))

        assert received == [1.234]

    def test_missing_last_size_defaults_to_zero(self) -> None:
        provider = self._make_provider()
        received: list[float] = []
        provider.on_quote(lambda sym, p, size, t: received.append(size))

        msg = self._make_ticker_msg()
        del msg["last_size"]
        provider._handle_ticker(msg)

        assert received == [0.0]

    def test_correct_timestamp_from_ticker(self) -> None:
        provider = self._make_provider()
        received: list[datetime] = []
        provider.on_quote(lambda sym, p, s, ts: received.append(ts))

        provider._handle_ticker(self._make_ticker_msg(time="2023-11-14T20:00:00Z"))

        expected = datetime(2023, 11, 14, 20, 0, 0, tzinfo=timezone.utc)
        assert received == [expected]

    def test_timestamp_is_utc_aware(self) -> None:
        provider = self._make_provider()
        received: list[datetime] = []
        provider.on_quote(lambda sym, p, s, ts: received.append(ts))

        provider._handle_ticker(self._make_ticker_msg())

        assert received[0].tzinfo is not None

    def test_invalid_time_falls_back_to_now(self) -> None:
        """Malformed timestamp should not raise; provider falls back to utcnow."""
        provider = self._make_provider()
        received: list[datetime] = []
        provider.on_quote(lambda sym, p, s, ts: received.append(ts))

        msg = self._make_ticker_msg(time="not-a-timestamp")
        provider._handle_ticker(msg)  # must not raise

        assert len(received) == 1
        assert received[0].tzinfo is not None

    def test_missing_time_field_falls_back_to_now(self) -> None:
        provider = self._make_provider()
        received: list[datetime] = []
        provider.on_quote(lambda sym, p, s, ts: received.append(ts))

        msg = self._make_ticker_msg()
        del msg["time"]
        provider._handle_ticker(msg)

        assert len(received) == 1
        assert received[0].tzinfo is not None

    def test_multiple_callbacks_all_invoked(self) -> None:
        provider = self._make_provider()
        cb1: QuoteCallback = MagicMock()
        cb2: QuoteCallback = MagicMock()
        provider.on_quote(cb1)
        provider.on_quote(cb2)

        provider._handle_ticker(self._make_ticker_msg())

        cb1.assert_called_once()
        cb2.assert_called_once()


# ===========================================================================
# 6. BaseStreamingProvider – _emit_quote and _emit_bar
# ===========================================================================

class TestBaseEmitQuote:
    """Tests for BaseStreamingProvider._emit_quote."""

    def _make_provider(self) -> ConcreteProvider:
        return ConcreteProvider()

    def _sample_ts(self) -> datetime:
        return datetime(2024, 1, 15, 10, 0, tzinfo=timezone.utc)

    def test_registered_callback_is_called(self) -> None:
        provider = self._make_provider()
        callback: QuoteCallback = MagicMock()
        provider.on_quote(callback)

        provider._emit_quote("BTC/USD", 45000.0, 1.0, self._sample_ts())

        callback.assert_called_once_with("BTC/USD", 45000.0, 1.0, self._sample_ts())

    def test_multiple_callbacks_all_called(self) -> None:
        provider = self._make_provider()
        cb1: QuoteCallback = MagicMock()
        cb2: QuoteCallback = MagicMock()
        cb3: QuoteCallback = MagicMock()
        provider.on_quote(cb1)
        provider.on_quote(cb2)
        provider.on_quote(cb3)

        provider._emit_quote("ETH/USD", 3000.0, 2.0, self._sample_ts())

        cb1.assert_called_once()
        cb2.assert_called_once()
        cb3.assert_called_once()

    def test_no_callbacks_does_not_raise(self) -> None:
        provider = self._make_provider()
        provider._emit_quote("BTC/USD", 45000.0, 1.0, self._sample_ts())  # no error

    def test_callback_that_raises_does_not_break_others(self) -> None:
        """An exception in one callback must not prevent subsequent callbacks."""
        provider = self._make_provider()
        results: list[str] = []

        def bad_callback(sym: str, p: float, s: float, ts: datetime) -> None:
            raise RuntimeError("boom")

        def good_callback(sym: str, p: float, s: float, ts: datetime) -> None:
            results.append(sym)

        provider.on_quote(bad_callback)
        provider.on_quote(good_callback)

        provider._emit_quote("BTC/USD", 45000.0, 1.0, self._sample_ts())

        assert results == ["BTC/USD"]

    def test_first_callback_raises_second_still_invoked(self) -> None:
        provider = self._make_provider()
        cb2: QuoteCallback = MagicMock()

        provider.on_quote(lambda sym, p, s, t: (_ for _ in ()).throw(ValueError("err")))
        provider.on_quote(cb2)

        provider._emit_quote("BTC/USD", 45000.0, 1.0, self._sample_ts())

        cb2.assert_called_once()

    def test_correct_arguments_forwarded(self) -> None:
        provider = self._make_provider()
        received: list[tuple] = []
        provider.on_quote(lambda sym, p, s, t: received.append((sym, p, s, t)))

        ts = datetime(2024, 6, 1, 9, 30, tzinfo=timezone.utc)
        provider._emit_quote("SOL/USD", 150.75, 10.0, ts)

        assert received == [("SOL/USD", 150.75, 10.0, ts)]


class TestBaseEmitBar:
    """Tests for BaseStreamingProvider._emit_bar."""

    def _make_provider(self) -> ConcreteProvider:
        return ConcreteProvider()

    def _sample_bar(self) -> Bar:
        return Bar(
            timestamp=datetime(2024, 1, 15, 9, 30, tzinfo=timezone.utc),
            open=45000.0,
            high=45500.0,
            low=44800.0,
            close=45200.0,
            volume=500.0,
        )

    def test_registered_callback_is_called(self) -> None:
        provider = self._make_provider()
        callback: BarCallback = MagicMock()
        provider.on_bar(callback)
        bar = self._sample_bar()

        provider._emit_bar("BTC/USD", bar)

        callback.assert_called_once_with("BTC/USD", bar)

    def test_multiple_callbacks_all_called(self) -> None:
        provider = self._make_provider()
        cb1: BarCallback = MagicMock()
        cb2: BarCallback = MagicMock()
        provider.on_bar(cb1)
        provider.on_bar(cb2)
        bar = self._sample_bar()

        provider._emit_bar("ETH/USD", bar)

        cb1.assert_called_once()
        cb2.assert_called_once()

    def test_no_callbacks_does_not_raise(self) -> None:
        provider = self._make_provider()
        provider._emit_bar("BTC/USD", self._sample_bar())  # no error

    def test_callback_that_raises_does_not_break_others(self) -> None:
        provider = self._make_provider()
        results: list[str] = []

        def bad_callback(sym: str, bar: Bar) -> None:
            raise RuntimeError("boom")

        def good_callback(sym: str, bar: Bar) -> None:
            results.append(sym)

        provider.on_bar(bad_callback)
        provider.on_bar(good_callback)
        bar = self._sample_bar()

        provider._emit_bar("BTC/USD", bar)

        assert results == ["BTC/USD"]

    def test_correct_bar_forwarded_to_callback(self) -> None:
        provider = self._make_provider()
        received: list[Bar] = []
        provider.on_bar(lambda sym, bar: received.append(bar))
        bar = self._sample_bar()

        provider._emit_bar("BTC/USD", bar)

        assert received == [bar]

    def test_correct_symbol_forwarded_to_callback(self) -> None:
        provider = self._make_provider()
        received: list[str] = []
        provider.on_bar(lambda sym, bar: received.append(sym))

        provider._emit_bar("SOL/USD", self._sample_bar())

        assert received == ["SOL/USD"]


# ===========================================================================
# 7. BaseStreamingProvider – state and subscription tracking
# ===========================================================================

class TestBaseStreamingProviderState:
    """Tests for is_connected and subscribed_symbols properties."""

    def test_initial_is_connected_false(self) -> None:
        provider = ConcreteProvider()
        assert provider.is_connected is False

    def test_subscribed_symbols_initially_empty(self) -> None:
        provider = ConcreteProvider()
        assert provider.subscribed_symbols == set()

    def test_subscribed_symbols_returns_copy(self) -> None:
        """Mutating the returned set must not affect internal state."""
        provider = ConcreteProvider()
        provider._subscribed_symbols.add("BTC/USD")
        external = provider.subscribed_symbols
        external.add("ETH/USD")
        assert "ETH/USD" not in provider._subscribed_symbols

    def test_on_quote_appends_callback(self) -> None:
        provider = ConcreteProvider()
        cb1: QuoteCallback = MagicMock()
        cb2: QuoteCallback = MagicMock()
        provider.on_quote(cb1)
        provider.on_quote(cb2)
        assert provider._quote_callbacks == [cb1, cb2]

    def test_on_bar_appends_callback(self) -> None:
        provider = ConcreteProvider()
        cb1: BarCallback = MagicMock()
        cb2: BarCallback = MagicMock()
        provider.on_bar(cb1)
        provider.on_bar(cb2)
        assert provider._bar_callbacks == [cb1, cb2]
