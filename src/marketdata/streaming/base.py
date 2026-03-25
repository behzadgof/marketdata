"""Base classes and callback types for streaming providers."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Callable

from marketdata.models.bar import Bar

# Callback types
QuoteCallback = Callable[[str, float, float, datetime], None]
"""(symbol, price, size, timestamp)"""

BarCallback = Callable[[str, Bar], None]
"""(symbol, bar)"""


class BaseStreamingProvider(ABC):
    """Abstract base class for real-time streaming data providers."""

    def __init__(self) -> None:
        self._quote_callbacks: list[QuoteCallback] = []
        self._bar_callbacks: list[BarCallback] = []
        self._connected = False
        self._subscribed_symbols: set[str] = set()

    @abstractmethod
    async def connect(self) -> None:
        """Open the streaming connection."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Close the streaming connection."""

    @abstractmethod
    async def subscribe(self, symbols: list[str], channels: list[str]) -> None:
        """Subscribe to real-time data for the given symbols."""

    @abstractmethod
    async def unsubscribe(self, symbols: list[str]) -> None:
        """Unsubscribe from the given symbols."""

    def on_quote(self, callback: QuoteCallback) -> None:
        """Register a callback for quote updates."""
        self._quote_callbacks.append(callback)

    def on_bar(self, callback: BarCallback) -> None:
        """Register a callback for bar updates."""
        self._bar_callbacks.append(callback)

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def subscribed_symbols(self) -> set[str]:
        return set(self._subscribed_symbols)

    def _emit_quote(self, symbol: str, price: float, size: float, timestamp: datetime) -> None:
        for cb in self._quote_callbacks:
            try:
                cb(symbol, price, size, timestamp)
            except Exception:
                pass

    def _emit_bar(self, symbol: str, bar: Bar) -> None:
        for cb in self._bar_callbacks:
            try:
                cb(symbol, bar)
            except Exception:
                pass
