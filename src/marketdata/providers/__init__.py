"""Market data provider registry."""

from __future__ import annotations

from marketdata.config import MarketDataProviderType
from marketdata.providers.base import BaseMarketDataProvider

# Lazy registry — actual classes imported on demand to avoid pulling in
# optional dependencies when they aren't used.
PROVIDER_CLASSES: dict[MarketDataProviderType, str] = {
    MarketDataProviderType.POLYGON: "marketdata.providers.polygon.PolygonProvider",
    MarketDataProviderType.ALPACA: "marketdata.providers.alpaca.AlpacaProvider",
    MarketDataProviderType.IB: "marketdata.providers.ib.IBProvider",
    MarketDataProviderType.FINNHUB: "marketdata.providers.finnhub.FinnhubProvider",
    MarketDataProviderType.MOCK: "marketdata.providers.mock.MockProvider",
}


def create_provider(
    provider_type: MarketDataProviderType,
    **kwargs,
) -> BaseMarketDataProvider:
    """Instantiate a provider by type, forwarding kwargs to its constructor."""
    import importlib

    dotted = PROVIDER_CLASSES[provider_type]
    module_path, cls_name = dotted.rsplit(".", 1)
    module = importlib.import_module(module_path)
    cls = getattr(module, cls_name)
    return cls(**kwargs)


__all__ = ["BaseMarketDataProvider", "PROVIDER_CLASSES", "create_provider"]
