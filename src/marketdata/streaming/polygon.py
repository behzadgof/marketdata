"""Polygon.io streaming provider (stub)."""

from __future__ import annotations

from marketdata.streaming.base import BaseStreamingProvider


class PolygonStreamingProvider(BaseStreamingProvider):
    """Polygon streaming provider — not yet implemented."""

    async def connect(self) -> None:
        raise NotImplementedError("Polygon streaming not yet implemented")

    async def disconnect(self) -> None:
        raise NotImplementedError

    async def subscribe(self, symbols: list[str], channels: list[str]) -> None:
        raise NotImplementedError

    async def unsubscribe(self, symbols: list[str]) -> None:
        raise NotImplementedError
