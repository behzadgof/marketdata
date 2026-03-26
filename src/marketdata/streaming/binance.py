"""Binance WebSocket streaming provider for real-time crypto data."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from marketdata.config import AssetType, detect_asset_type
from marketdata.streaming.base import BaseStreamingProvider

logger = logging.getLogger(__name__)

_BINANCE_WS_URLS = [
    "wss://stream.binance.us:9443/ws",
    "wss://stream.binance.com:9443/ws",
]


class BinanceStreamingProvider(BaseStreamingProvider):
    """Real-time crypto data via Binance WebSocket API.

    Supports trade streams for crypto symbols. No API key required
    for public market data.
    """

    def __init__(self, url: str | None = None) -> None:
        super().__init__()
        self._url = url  # resolved in connect() if None
        self._urls = _BINANCE_WS_URLS
        self._ws: Any = None
        self._recv_task: asyncio.Task | None = None
        self._msg_id = 0

    # -- Symbol mapping -------------------------------------------------------

    @staticmethod
    def _to_binance_symbol(symbol: str) -> str:
        """Convert user-facing symbol to Binance stream format.

        BTC/USD → btcusdt, ETH → ethusdt
        """
        upper = symbol.upper().replace("/", "")
        if upper.endswith("USD") and not upper.endswith("USDT"):
            upper = upper + "T"  # USD → USDT
        return upper.lower()

    @staticmethod
    def _from_binance_symbol(symbol: str) -> str:
        """Convert Binance symbol back to user-facing format.

        BTCUSDT → BTC/USD
        """
        upper = symbol.upper()
        if upper.endswith("USDT"):
            base = upper[:-4]
            return f"{base}/USD"
        return upper

    # -- Connection lifecycle -------------------------------------------------

    async def connect(self) -> None:
        import websockets

        urls = [self._url] if self._url else self._urls
        last_exc: Exception | None = None
        for url in urls:
            try:
                self._ws = await asyncio.wait_for(
                    websockets.connect(url), timeout=5,
                )
                self._connected = True
                self._recv_task = asyncio.create_task(self._recv_loop())
                logger.info("Binance WebSocket connected to %s", url)
                return
            except Exception as exc:
                last_exc = exc
                logger.debug("Binance %s failed: %s", url, exc)
        raise last_exc or ConnectionError("No Binance WebSocket URL available")

    async def disconnect(self) -> None:
        self._connected = False
        if self._recv_task and not self._recv_task.done():
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()
            self._ws = None
        logger.info("Binance WebSocket disconnected")

    # -- Subscription ---------------------------------------------------------

    async def subscribe(self, symbols: list[str], channels: list[str]) -> None:
        streams = []
        for sym in symbols:
            if detect_asset_type(sym) != AssetType.CRYPTO:
                continue
            binance_sym = self._to_binance_symbol(sym)
            if "quotes" in channels or "trades" in channels:
                streams.append(f"{binance_sym}@miniTicker")
            self._subscribed_symbols.add(sym)

        if streams and self._ws:
            self._msg_id += 1
            await self._ws.send(json.dumps({
                "method": "SUBSCRIBE",
                "params": streams,
                "id": self._msg_id,
            }))

    async def unsubscribe(self, symbols: list[str]) -> None:
        streams = []
        for sym in symbols:
            binance_sym = self._to_binance_symbol(sym)
            streams.append(f"{binance_sym}@miniTicker")
            self._subscribed_symbols.discard(sym)

        if streams and self._ws:
            self._msg_id += 1
            await self._ws.send(json.dumps({
                "method": "UNSUBSCRIBE",
                "params": streams,
                "id": self._msg_id,
            }))

    # -- Receive loop ---------------------------------------------------------

    async def _recv_loop(self) -> None:
        retry_delay = 1.0
        max_delay = 60.0
        while True:
            try:
                async for raw in self._ws:
                    try:
                        msg = json.loads(raw)
                        event = msg.get("e", "")
                        if event == "trade":
                            self._handle_trade(msg)
                        elif event == "24hrMiniTicker":
                            self._handle_mini_ticker(msg)
                    except Exception:
                        pass
                self._connected = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Binance recv loop error: %s", exc)
                self._connected = False

            # Auto-reconnect
            logger.info("Binance disconnected, reconnecting in %.0fs...", retry_delay)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_delay)
            try:
                import websockets
                urls = [self._url] if self._url else self._urls
                for url in urls:
                    try:
                        self._ws = await asyncio.wait_for(
                            websockets.connect(url), timeout=5,
                        )
                        self._connected = True
                        retry_delay = 1.0
                        logger.info("Binance reconnected to %s", url)
                        # Re-subscribe
                        if self._subscribed_symbols:
                            streams = [
                                f"{self._to_binance_symbol(s)}@miniTicker"
                                for s in self._subscribed_symbols
                            ]
                            self._msg_id += 1
                            await self._ws.send(json.dumps({
                                "method": "SUBSCRIBE",
                                "params": streams,
                                "id": self._msg_id,
                            }))
                        break
                    except Exception:
                        continue
                else:
                    logger.warning("Binance reconnect failed, all URLs exhausted")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Binance reconnect failed: %s", exc)

    def _handle_trade(self, msg: dict) -> None:
        binance_sym = msg.get("s", "")
        symbol = self._from_binance_symbol(binance_sym)
        price = float(msg["p"])
        size = float(msg["q"])
        ts_ms = msg.get("T", 0)
        timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        self._emit_quote(symbol, price, size, timestamp)

    def _handle_mini_ticker(self, msg: dict) -> None:
        """Handle 24hr mini ticker: {e, s, c (close), o (open), h, l, v, q}."""
        binance_sym = msg.get("s", "")
        symbol = self._from_binance_symbol(binance_sym)
        price = float(msg["c"])  # current/close price
        volume = float(msg.get("v", 0))
        ts_ms = msg.get("E", 0)  # event time
        timestamp = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        self._emit_quote(symbol, price, volume, timestamp)
