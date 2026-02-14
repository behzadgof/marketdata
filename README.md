# marketdata

Unified market data SDK for US equities — multi-provider, caching, quality validation.

## Quick Start

```python
from marketdata import create_manager_from_env

mgr = create_manager_from_env()
bars = mgr.get_bars("AAPL", date(2024, 1, 2), date(2024, 1, 5))
quote = mgr.get_quote("AAPL")
info = mgr.get_ticker_info("AAPL")
```

## Providers

| Provider | Bars | Quotes | Snapshots | Ticker Info | Earnings | Dividends |
|----------|------|--------|-----------|-------------|----------|-----------|
| Polygon  | Y    | Y      | Y         | Y           | Y        | Y         |
| Alpaca   | Y    | Y      | Y         |             |          |           |
| IB       | Y    | Y      |           | Y           |          | Y         |
| Finnhub  | Y    |        |           | Y           | Y        |           |
| Mock     | Y    | Y      | Y         | Y           | Y        | Y         |

## Install

```bash
pip install marketdata                # core (requests only)
pip install marketdata[polygon]       # + Polygon SDK
pip install marketdata[alpaca]        # + Alpaca SDK
pip install marketdata[all]           # all providers
```

## Environment Variables

```
MARKET_DATA_PROVIDERS=polygon,alpaca   # comma-separated, priority order
POLYGON_API_KEY=...
ALPACA_API_KEY=...
ALPACA_SECRET_KEY=...
FINNHUB_API_KEY=...
```
