"""marketdata — Unified market data SDK for US equities.

Multi-provider (Polygon, Alpaca, IB, Finnhub), automatic fallback,
caching, data quality validation, and rich data models.

Quick start::

    from marketdata import create_manager_from_env
    mgr = create_manager_from_env()
    bars = mgr.get_bars("AAPL", date(2024, 1, 2), date(2024, 1, 5))
"""

from __future__ import annotations

import os

from marketdata.compat import DataConfig, DataManager, DataValidator, ParquetStorage
from marketdata.config import MarketDataConfig, MarketDataProviderType
from marketdata.earnings_calendar import (
    EarningsCalendar,
    EarningsCallTime,
    EarningsContext,
    EarningsFetcher,
    get_earnings_context,
    load_earnings_calendar,
)
from marketdata.errors import MarketDataError, MarketDataErrorCode
from marketdata.manager import MarketDataManager
from marketdata.models.bar import Bar
from marketdata.models.corporate_action import CorporateAction
from marketdata.models.dividend import DividendEvent
from marketdata.models.earnings import EarningsEvent
from marketdata.models.quote import Quote
from marketdata.models.snapshot import Snapshot
from marketdata.models.ticker_info import TickerInfo
from marketdata.provider_settings import (
    DEFAULT_PROVIDER_ORDER,
    PROVIDER_SPECS,
    MarketDataProviderSettings,
    ProviderFieldSpec,
    ProviderSettingsError,
    ProviderSpec,
)

__version__ = "0.1.0"

__all__ = [
    # Manager
    "MarketDataManager",
    "create_manager_from_env",
    # Compatibility APIs
    "DataManager",
    "DataConfig",
    "ParquetStorage",
    "DataValidator",
    # Provider settings
    "MarketDataProviderSettings",
    "ProviderSettingsError",
    "ProviderFieldSpec",
    "ProviderSpec",
    "PROVIDER_SPECS",
    "DEFAULT_PROVIDER_ORDER",
    # Config
    "MarketDataConfig",
    "MarketDataProviderType",
    # Errors
    "MarketDataError",
    "MarketDataErrorCode",
    # Models
    "Bar",
    "Quote",
    "Snapshot",
    "TickerInfo",
    "EarningsEvent",
    "DividendEvent",
    "CorporateAction",
    # Earnings calendar utilities
    "EarningsCallTime",
    "EarningsContext",
    "EarningsCalendar",
    "EarningsFetcher",
    "load_earnings_calendar",
    "get_earnings_context",
]


def create_manager_from_env() -> MarketDataManager:
    """Zero-config factory — reads provider list and API keys from env vars.

    Environment variables:
        MARKET_DATA_PROVIDERS: Comma-separated provider list (default: "polygon").
        MARKET_DATA_CACHE: Cache backend — "parquet", "memory", "none" (default: "parquet").
        MARKET_DATA_CACHE_DIR: Cache directory (default: "data/cache").
        POLYGON_API_KEY: Polygon.io API key.
        ALPACA_API_KEY: Alpaca API key.
        ALPACA_SECRET_KEY: Alpaca API secret.
        FINNHUB_API_KEY: Finnhub API key.
        IB_HOST: IB TWS/Gateway host (default: "127.0.0.1").
        IB_PORT: IB TWS/Gateway port (default: 7497).
        IB_CLIENT_ID: IB client ID (default: 1).
    """
    provider_str = os.getenv("MARKET_DATA_PROVIDERS", "polygon")
    provider_types = [
        MarketDataProviderType(name.strip())
        for name in provider_str.split(",")
        if name.strip()
    ]

    config = MarketDataConfig(
        providers=provider_types,
        cache_backend=os.getenv("MARKET_DATA_CACHE", "parquet"),
        cache_dir=os.getenv("MARKET_DATA_CACHE_DIR", "data/cache"),
        polygon_api_key=os.getenv("POLYGON_API_KEY"),
        alpaca_api_key=os.getenv("ALPACA_API_KEY"),
        alpaca_api_secret=os.getenv("ALPACA_SECRET_KEY"),
        finnhub_api_key=os.getenv("FINNHUB_API_KEY"),
        ib_host=os.getenv("IB_HOST", "127.0.0.1"),
        ib_port=int(os.getenv("IB_PORT", "7497")),
        ib_client_id=int(os.getenv("IB_CLIENT_ID", "1")),
    )

    return MarketDataManager(config)
