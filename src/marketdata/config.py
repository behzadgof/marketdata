"""Market data configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class MarketDataProviderType(Enum):
    """Supported data provider backends."""

    POLYGON = "polygon"
    ALPACA = "alpaca"
    IB = "ib"
    FINNHUB = "finnhub"
    MOCK = "mock"


@dataclass
class MarketDataConfig:
    """Configuration for MarketDataManager.

    Attributes:
        providers: Provider backends ordered by priority.
        cache_backend: Cache type — "parquet", "memory", or "none".
        cache_dir: Directory for parquet cache files.
        cache_ttl_seconds: TTL for in-memory cache entries (quotes/snapshots).
        validate: Whether to run quality checks on fetched data.
        polygon_api_key: Polygon.io API key.
        alpaca_api_key: Alpaca API key.
        alpaca_api_secret: Alpaca API secret.
        finnhub_api_key: Finnhub API key.
        ib_host: Interactive Brokers TWS/Gateway host.
        ib_port: Interactive Brokers TWS/Gateway port.
        ib_client_id: Interactive Brokers client ID.
    """

    providers: list[MarketDataProviderType] = field(
        default_factory=lambda: [MarketDataProviderType.POLYGON]
    )
    cache_backend: str = "parquet"
    cache_dir: str = "data/cache"
    cache_ttl_seconds: int = 60
    validate: bool = True

    polygon_api_key: str | None = None
    alpaca_api_key: str | None = None
    alpaca_api_secret: str | None = None
    finnhub_api_key: str | None = None
    ib_host: str = "127.0.0.1"
    ib_port: int = 7497
    ib_client_id: int = 1
