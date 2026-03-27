"""Stream manager for coordinating multiple streaming providers."""

from __future__ import annotations

from marketdata.streaming.base import BarCallback, BaseStreamingProvider, QuoteCallback


class StreamManager:
    """Manages multiple streaming providers and routes subscriptions."""

    def __init__(self, providers: list[BaseStreamingProvider]) -> None:
        self._providers = list(providers)

    @property
    def has_providers(self) -> bool:
        return bool(self._providers)

    @property
    def is_connected(self) -> bool:
        return any(p.is_connected for p in self._providers)

    async def connect(self) -> None:
        for p in self._providers:
            await p.connect()

    async def disconnect(self) -> None:
        for p in self._providers:
            await p.disconnect()

    async def subscribe(self, symbols: list[str], channels: list[str]) -> None:
        for p in self._providers:
            await p.subscribe(symbols, channels)

    async def unsubscribe(self, symbols: list[str]) -> None:
        for p in self._providers:
            await p.unsubscribe(symbols)

    def on_quote(self, callback: QuoteCallback) -> None:
        for p in self._providers:
            p.on_quote(callback)

    def on_bar(self, callback: BarCallback) -> None:
        for p in self._providers:
            p.on_bar(callback)
