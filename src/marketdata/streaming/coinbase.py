"""Coinbase Exchange WebSocket streaming provider for real-time crypto data."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from marketdata.streaming.base import BaseStreamingProvider

logger = logging.getLogger(__name__)

_COINBASE_WS_URL = "wss://ws-feed.exchange.coinbase.com"


class CoinbaseStreamingProvider(BaseStreamingProvider):
    """Real-time crypto data via Coinbase Exchange WebSocket API.

    Free, no API key required, no geo-restrictions.
    Uses the 'ticker' channel for real-time price updates.
    """

    def __init__(self, url: str = _COINBASE_WS_URL) -> None:
        super().__init__()
        self._url = url
        self._ws: Any = None
        self._recv_task: asyncio.Task | None = None

    # -- Symbol mapping -------------------------------------------------------

    @staticmethod
    def _to_coinbase_symbol(symbol: str) -> str:
        """Convert user-facing symbol to Coinbase product ID.

        BTC/USD → BTC-USD, ETH/USD → ETH-USD
        """
        upper = symbol.upper()
        if "/" in upper:
            return upper.replace("/", "-")
        return f"{upper}-USD"

    @staticmethod
    def _from_coinbase_symbol(product_id: str) -> str:
        """Convert Coinbase product ID to user-facing symbol.

        BTC-USD → BTC/USD
        """
        return product_id.upper().replace("-", "/")

    # -- Connection lifecycle -------------------------------------------------

    async def connect(self) -> None:
        import websockets

        self._ws = await asyncio.wait_for(
            websockets.connect(self._url), timeout=10,
        )
        self._connected = True
        self._recv_task = asyncio.create_task(self._recv_loop())
        logger.info("Coinbase WebSocket connected")

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
        logger.info("Coinbase WebSocket disconnected")

    # -- Subscription ---------------------------------------------------------

    async def subscribe(self, symbols: list[str], channels: list[str]) -> None:
        product_ids = []
        for sym in symbols:
            product_ids.append(self._to_coinbase_symbol(sym))
            self._subscribed_symbols.add(sym)

        if product_ids and self._ws:
            await self._ws.send(json.dumps({
                "type": "subscribe",
                "channels": [
                    {"name": "ticker", "product_ids": product_ids}
                ],
            }))

    async def unsubscribe(self, symbols: list[str]) -> None:
        product_ids = []
        for sym in symbols:
            product_ids.append(self._to_coinbase_symbol(sym))
            self._subscribed_symbols.discard(sym)

        if product_ids and self._ws:
            await self._ws.send(json.dumps({
                "type": "unsubscribe",
                "channels": [
                    {"name": "ticker", "product_ids": product_ids}
                ],
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
                        if msg.get("type") == "ticker":
                            self._handle_ticker(msg)
                    except Exception:
                        pass
                # WebSocket closed normally — reconnect
                self._connected = False
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Coinbase recv loop error: %s", exc)
                self._connected = False

            # Auto-reconnect with exponential backoff
            logger.info("Coinbase disconnected, reconnecting in %.0fs...", retry_delay)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_delay)
            try:
                import websockets
                self._ws = await asyncio.wait_for(
                    websockets.connect(self._url), timeout=10,
                )
                self._connected = True
                retry_delay = 1.0
                logger.info("Coinbase reconnected")
                # Re-subscribe to previously subscribed symbols
                if self._subscribed_symbols:
                    product_ids = [
                        self._to_coinbase_symbol(s) for s in self._subscribed_symbols
                    ]
                    await self._ws.send(json.dumps({
                        "type": "subscribe",
                        "channels": [
                            {"name": "ticker", "product_ids": product_ids}
                        ],
                    }))
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning("Coinbase reconnect failed: %s", exc)

    def _handle_ticker(self, msg: dict) -> None:
        """Handle ticker message: {type, product_id, price, last_size, time}."""
        product_id = msg.get("product_id", "")
        symbol = self._from_coinbase_symbol(product_id)
        price = float(msg["price"])
        size = float(msg.get("last_size", 0))
        time_str = msg.get("time", "")
        try:
            timestamp = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            timestamp = datetime.now(timezone.utc)
        self._emit_quote(symbol, price, size, timestamp)
