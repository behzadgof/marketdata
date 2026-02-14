"""Cache backends for market data — Parquet (disk) and Memory (TTL)."""

from __future__ import annotations

import shutil
import time
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from marketdata.models.bar import Bar


class CacheBackend(ABC):
    """Abstract cache interface."""

    @abstractmethod
    def get_bars(
        self, symbol: str, start: date, end: date, timeframe: str,
    ) -> list[Bar] | None:
        """Return cached bars, or None on miss."""
        ...

    @abstractmethod
    def store_bars(
        self, symbol: str, bars: list[Bar], timeframe: str, start: date, end: date,
    ) -> None:
        """Store bars in cache."""
        ...

    @abstractmethod
    def has_data(self, symbol: str, timeframe: str, start: date, end: date) -> bool:
        ...

    @abstractmethod
    def clear(self, symbol: str) -> None:
        ...

    @abstractmethod
    def clear_all(self) -> None:
        ...


class NoCache(CacheBackend):
    """No-op cache — always misses."""

    def get_bars(self, symbol, start, end, timeframe):  # type: ignore[override]
        return None

    def store_bars(self, symbol, bars, timeframe, start, end):  # type: ignore[override]
        pass

    def has_data(self, symbol, timeframe, start, end):  # type: ignore[override]
        return False

    def clear(self, symbol):  # type: ignore[override]
        pass

    def clear_all(self):
        pass


class ParquetCache(CacheBackend):
    """Disk-based cache using Parquet files with Snappy compression.

    Storage layout: ``{base_path}/{SYMBOL}/{timeframe}_{start}_{end}.parquet``
    """

    def __init__(self, base_path: Path | str) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _file_path(
        self, symbol: str, timeframe: str, start: date, end: date,
    ) -> Path:
        symbol_dir = self.base_path / symbol.upper()
        symbol_dir.mkdir(exist_ok=True)
        return symbol_dir / f"{timeframe}_{start}_{end}.parquet"

    def get_bars(
        self, symbol: str, start: date, end: date, timeframe: str,
    ) -> list[Bar] | None:
        fp = self._file_path(symbol, timeframe, start, end)
        if not fp.exists():
            return None

        try:
            df = pd.read_parquet(fp)
            return self._df_to_bars(df)
        except Exception:
            return None

    def store_bars(
        self, symbol: str, bars: list[Bar], timeframe: str, start: date, end: date,
    ) -> None:
        if not bars:
            return
        fp = self._file_path(symbol, timeframe, start, end)
        df = self._bars_to_df(bars)
        df.to_parquet(fp, compression="snappy")

    def has_data(self, symbol: str, timeframe: str, start: date, end: date) -> bool:
        return self._file_path(symbol, timeframe, start, end).exists()

    def clear(self, symbol: str) -> None:
        symbol_dir = self.base_path / symbol.upper()
        if symbol_dir.exists():
            shutil.rmtree(symbol_dir)

    def clear_all(self) -> None:
        for d in self.base_path.iterdir():
            if d.is_dir():
                shutil.rmtree(d)

    # ---- helpers ----

    @staticmethod
    def _bars_to_df(bars: list[Bar]) -> pd.DataFrame:
        records = [
            {
                "timestamp": b.timestamp,
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume,
                "vwap": b.vwap,
                "num_trades": b.num_trades,
            }
            for b in bars
        ]
        return pd.DataFrame(records)

    @staticmethod
    def _df_to_bars(df: pd.DataFrame) -> list[Bar]:
        bars: list[Bar] = []
        for _, row in df.iterrows():
            bars.append(Bar(
                timestamp=pd.Timestamp(row["timestamp"]).to_pydatetime(),
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
                vwap=float(row["vwap"]) if pd.notna(row.get("vwap")) else None,
                num_trades=int(row["num_trades"]) if pd.notna(row.get("num_trades")) else None,
            ))
        return bars


class MemoryCache(CacheBackend):
    """In-memory TTL cache for bars, quotes, and snapshots.

    Uses LRU eviction when ``max_entries`` is exceeded.
    """

    def __init__(self, ttl_seconds: int = 60, max_entries: int = 1000) -> None:
        self.ttl = ttl_seconds
        self.max_entries = max_entries
        self._store: OrderedDict[str, tuple[float, Any]] = OrderedDict()

    def _key(self, symbol: str, timeframe: str, start: date, end: date) -> str:
        return f"{symbol.upper()}|{timeframe}|{start}|{end}"

    def _evict_expired(self) -> None:
        now = time.monotonic()
        expired = [k for k, (ts, _) in self._store.items() if now - ts > self.ttl]
        for k in expired:
            del self._store[k]

    def _evict_lru(self) -> None:
        while len(self._store) > self.max_entries:
            self._store.popitem(last=False)

    def get_bars(
        self, symbol: str, start: date, end: date, timeframe: str,
    ) -> list[Bar] | None:
        self._evict_expired()
        key = self._key(symbol, timeframe, start, end)
        entry = self._store.get(key)
        if entry is None:
            return None
        ts, bars = entry
        if time.monotonic() - ts > self.ttl:
            del self._store[key]
            return None
        self._store.move_to_end(key)  # refresh LRU position
        return bars

    def store_bars(
        self, symbol: str, bars: list[Bar], timeframe: str, start: date, end: date,
    ) -> None:
        key = self._key(symbol, timeframe, start, end)
        self._store[key] = (time.monotonic(), bars)
        self._store.move_to_end(key)
        self._evict_lru()

    def has_data(self, symbol: str, timeframe: str, start: date, end: date) -> bool:
        self._evict_expired()
        return self._key(symbol, timeframe, start, end) in self._store

    def clear(self, symbol: str) -> None:
        prefix = f"{symbol.upper()}|"
        keys = [k for k in self._store if k.startswith(prefix)]
        for k in keys:
            del self._store[k]

    def clear_all(self) -> None:
        self._store.clear()
