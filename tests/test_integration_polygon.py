"""Integration test for Polygon provider with 3 real assets.

Requires POLYGON_API_KEY in .env or environment.
Run: python -m pytest tests/test_integration_polygon.py -v -s
"""

import os
import sys
from datetime import date, timedelta

# Load .env from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))
except ImportError:
    pass

from marketdata.providers.polygon import PolygonProvider
from marketdata.models.bar import Bar
from marketdata.models.quote import Quote
from marketdata.models.snapshot import Snapshot
from marketdata.models.ticker_info import TickerInfo
from marketdata.models.earnings import EarningsEvent
from marketdata.models.dividend import DividendEvent

SYMBOLS = ["AAPL", "MSFT", "GOOGL"]

# Use a recent trading day (avoid weekends)
def last_trading_day() -> date:
    today = date.today()
    # Go back to find a weekday
    d = today - timedelta(days=1)
    while d.weekday() >= 5:  # Sat=5, Sun=6
        d -= timedelta(days=1)
    return d

END = last_trading_day()
START = END - timedelta(days=5)  # ~1 week of bars


def main():
    api_key = os.getenv("POLYGON_API_KEY")
    if not api_key:
        print("POLYGON_API_KEY not set. Skipping integration test.")
        return

    provider = PolygonProvider(api_key=api_key)
    print(f"Provider created. Capabilities: {provider.capabilities()}")
    print(f"Date range: {START} to {END}")
    print("=" * 70)

    results = {}

    for sym in SYMBOLS:
        print(f"\n{'='*70}")
        print(f"  {sym}")
        print(f"{'='*70}")
        sym_results = {}

        # 1. get_bars
        print(f"\n--- get_bars({sym}, {START}, {END}, '1day') ---")
        try:
            bars = provider.get_bars(sym, START, END, "1day")
            print(f"  Received {len(bars)} daily bars")
            assert len(bars) > 0, "Expected at least 1 bar"
            for b in bars[:3]:
                assert isinstance(b, Bar)
                print(f"  {b.timestamp.date()} O={b.open:.2f} H={b.high:.2f} L={b.low:.2f} C={b.close:.2f} V={b.volume:.0f} VWAP={b.vwap} trades={b.num_trades}")
                assert b.open > 0
                assert b.high >= b.low
                assert b.volume >= 0
            sym_results["bars"] = f"OK ({len(bars)} bars)"
        except Exception as e:
            print(f"  FAILED: {e}")
            sym_results["bars"] = f"FAIL: {e}"

        # 2. get_quote
        print(f"\n--- get_quote({sym}) ---")
        try:
            quote = provider.get_quote(sym)
            assert isinstance(quote, Quote)
            print(f"  bid={quote.bid_price:.2f}x{quote.bid_size} ask={quote.ask_price:.2f}x{quote.ask_size}")
            print(f"  last={quote.last_price} spread={quote.spread:.4f} mid={quote.mid_price:.2f}")
            sym_results["quote"] = "OK"
        except Exception as e:
            print(f"  FAILED: {e}")
            sym_results["quote"] = f"FAIL: {e}"

        # 3. get_ticker_info
        print(f"\n--- get_ticker_info({sym}) ---")
        try:
            info = provider.get_ticker_info(sym)
            assert isinstance(info, TickerInfo)
            print(f"  name={info.name}")
            print(f"  type={info.type} exchange={info.exchange}")
            print(f"  sector={info.sector} industry={info.industry}")
            print(f"  market_cap={info.market_cap:,.0f}" if info.market_cap else "  market_cap=None")
            print(f"  cik={info.cik} figi={info.composite_figi}")
            print(f"  shares_outstanding={info.shares_outstanding:,.0f}" if info.shares_outstanding else "  shares_outstanding=None")
            sym_results["ticker_info"] = "OK"
        except Exception as e:
            print(f"  FAILED: {e}")
            sym_results["ticker_info"] = f"FAIL: {e}"

        # 4. get_earnings
        print(f"\n--- get_earnings({sym}, limit=4) ---")
        try:
            earnings = provider.get_earnings(sym, limit=4)
            print(f"  Received {len(earnings)} earnings events")
            for ev in earnings[:4]:
                assert isinstance(ev, EarningsEvent)
                print(f"  {ev.report_date} Q{ev.fiscal_quarter}/{ev.fiscal_year} EPS_est={ev.eps_estimate} EPS_act={ev.eps_actual}")
            sym_results["earnings"] = f"OK ({len(earnings)} events)"
        except Exception as e:
            print(f"  FAILED: {e}")
            sym_results["earnings"] = f"FAIL: {e}"

        # 5. get_dividends
        print(f"\n--- get_dividends({sym}, limit=4) ---")
        try:
            divs = provider.get_dividends(sym, limit=4)
            print(f"  Received {len(divs)} dividend events")
            for d in divs[:4]:
                assert isinstance(d, DividendEvent)
                print(f"  ex={d.ex_date} amount=${d.amount:.4f} type={d.dividend_type} freq={d.frequency}")
            sym_results["dividends"] = f"OK ({len(divs)} events)"
        except Exception as e:
            print(f"  FAILED: {e}")
            sym_results["dividends"] = f"FAIL: {e}"

        results[sym] = sym_results

    # Summary
    print(f"\n\n{'='*70}")
    print("  INTEGRATION TEST SUMMARY")
    print(f"{'='*70}")
    print(f"{'Method':<20}", end="")
    for sym in SYMBOLS:
        print(f"{sym:<20}", end="")
    print()
    print("-" * 80)

    all_pass = True
    for method in ["bars", "quote", "ticker_info", "earnings", "dividends"]:
        print(f"{method:<20}", end="")
        for sym in SYMBOLS:
            status = results.get(sym, {}).get(method, "SKIP")
            is_ok = status.startswith("OK")
            marker = "PASS" if is_ok else "FAIL"
            if not is_ok:
                all_pass = False
            print(f"{marker:<20}", end="")
        print()

    print(f"\n{'='*70}")
    print(f"  RESULT: {'ALL PASSED' if all_pass else 'SOME FAILURES'}")
    print(f"{'='*70}")

    if not all_pass:
        sys.exit(1)


if __name__ == "__main__":
    main()
