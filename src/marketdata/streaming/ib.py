"""Interactive Brokers streaming provider (stub)."""

from __future__ import annotations

from marketdata.streaming.base import BaseStreamingProvider


class IBStreamingProvider(BaseStreamingProvider):
    """IB streaming provider — not yet implemented."""

    async def connect(self) -> None:
        raise NotImplementedError("IB streaming not yet implemented")

    async def disconnect(self) -> None:
        raise NotImplementedError

    async def subscribe(self, symbols: list[str], channels: list[str]) -> None:
        raise NotImplementedError

    async def unsubscribe(self, symbols: list[str]) -> None:
        raise NotImplementedError
