"""Compatibility APIs for DataFrame-oriented market-data workflows.

This module preserves the older DataFrame/cache management interface used by
applications that have not yet migrated to ``MarketDataManager``'s typed
model API.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from marketdata.models.bar import Bar
from marketdata.providers.polygon import PolygonProvider


@dataclass
class DataConfig:
    """Configuration for ``DataManager`` compatibility API."""

    cache_path: Path = Path("data/cache")


class ParquetStorage:
    """Store and retrieve market data as Parquet files."""

    def __init__(self, base_path: Path | str) -> None:
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _get_file_path(
        self,
        symbol: str,
        timeframe: str = "1min",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Path:
        symbol_dir = self.base_path / symbol.upper()
        symbol_dir.mkdir(exist_ok=True)

        if start_date and end_date:
            filename = f"{timeframe}_{start_date}_{end_date}.parquet"
        else:
            filename = f"{timeframe}.parquet"

        return symbol_dir / filename

    def save(
        self,
        symbol: str,
        df: pd.DataFrame,
        timeframe: str = "1min",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Path:
        file_path = self._get_file_path(symbol, timeframe, start_date, end_date)
        df.to_parquet(file_path, compression="snappy")
        return file_path

    def load(
        self,
        symbol: str,
        timeframe: str = "1min",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> pd.DataFrame:
        symbol_dir = self.base_path / symbol.upper()
        if not symbol_dir.exists():
            raise FileNotFoundError(f"No data found for {symbol}")

        pattern = f"{timeframe}*.parquet"
        files = list(symbol_dir.glob(pattern))
        if not files:
            raise FileNotFoundError(f"No {timeframe} data found for {symbol}")

        dfs = [pd.read_parquet(f) for f in sorted(files)]
        df = pd.concat(dfs, ignore_index=True) if len(dfs) > 1 else dfs[0]

        if start_date or end_date:
            if "timestamp" in df.columns:
                # Handle both tz-naive and tz-aware timestamp encodings.
                df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
                if start_date:
                    df = df[df["timestamp"].dt.date >= start_date]
                if end_date:
                    df = df[df["timestamp"].dt.date <= end_date]

        return df

    def has_data(
        self,
        symbol: str,
        timeframe: str = "1min",
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> bool:
        symbol_dir = self.base_path / symbol.upper()
        if not symbol_dir.exists():
            return False

        if start_date and end_date:
            exact_file = symbol_dir / f"{timeframe}_{start_date}_{end_date}.parquet"
            return exact_file.exists()

        pattern = f"{timeframe}*.parquet"
        return len(list(symbol_dir.glob(pattern))) > 0

    def get_info(self, symbol: str) -> Optional[dict[str, Any]]:
        symbol_dir = self.base_path / symbol.upper()
        if not symbol_dir.exists():
            return None

        files = list(symbol_dir.glob("*.parquet"))
        if not files:
            return None

        total_size = sum(f.stat().st_size for f in files)
        try:
            df = pd.read_parquet(files[0])
            if "timestamp" in df.columns:
                timestamps = pd.to_datetime(df["timestamp"])
                start_dt = timestamps.min().date()
                end_dt = timestamps.max().date()
            else:
                start_dt = None
                end_dt = None
            bar_count = len(df)
        except Exception:
            start_dt = None
            end_dt = None
            bar_count = 0

        return {
            "symbol": symbol.upper(),
            "start_date": start_dt,
            "end_date": end_dt,
            "bar_count": bar_count,
            "size_mb": total_size / (1024 * 1024),
            "files": len(files),
        }

    def get_total_size_mb(self) -> float:
        total = 0
        for file in self.base_path.rglob("*.parquet"):
            total += file.stat().st_size
        return total / (1024 * 1024)

    def list_all(self) -> list[dict[str, Any]]:
        result = []
        for symbol_dir in self.base_path.iterdir():
            if symbol_dir.is_dir():
                info = self.get_info(symbol_dir.name)
                if info:
                    result.append(info)
        return result

    def clear(self, symbol: str) -> None:
        symbol_dir = self.base_path / symbol.upper()
        if symbol_dir.exists():
            shutil.rmtree(symbol_dir)

    def clear_all(self) -> None:
        for symbol_dir in self.base_path.iterdir():
            if symbol_dir.is_dir():
                shutil.rmtree(symbol_dir)


class DataManager:
    """DataFrame-oriented manager for download/cache workflows."""

    def __init__(self, config: Optional[DataConfig] = None) -> None:
        self.config = config or DataConfig()
        self.storage = ParquetStorage(self.config.cache_path)
        self._provider: PolygonProvider | None = None

    def _get_provider(self) -> PolygonProvider:
        if self._provider is None:
            self._provider = PolygonProvider()
        return self._provider

    @staticmethod
    def _bars_to_df(bars: list[Bar]) -> pd.DataFrame:
        if not bars:
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume", "vwap", "trades"]
            )

        records = []
        for b in bars:
            records.append(
                {
                    "timestamp": b.timestamp,
                    "open": float(b.open),
                    "high": float(b.high),
                    "low": float(b.low),
                    "close": float(b.close),
                    "volume": float(b.volume),
                    "vwap": float(b.vwap) if b.vwap is not None else None,
                    "trades": int(b.num_trades) if b.num_trades is not None else None,
                }
            )
        return pd.DataFrame(records)

    def download_historical(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1min",
    ) -> pd.DataFrame:
        provider = self._get_provider()
        bars = provider.get_bars(
            symbol=symbol,
            start=start_date,
            end=end_date,
            timeframe=timeframe,
        )
        df = self._bars_to_df(bars)
        self.storage.save(symbol, df, timeframe, start_date, end_date)
        return df

    def get_bars(
        self,
        symbol: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        timeframe: str = "1min",
    ) -> pd.DataFrame:
        return self.storage.load(symbol, timeframe, start_date, end_date)

    def has_cached_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date,
        timeframe: str = "1min",
    ) -> bool:
        return self.storage.has_data(symbol, timeframe, start_date, end_date)

    def get_cache_info(self, symbol: str) -> Optional[dict[str, Any]]:
        return self.storage.get_info(symbol)

    def get_total_cache_size(self) -> float:
        return self.storage.get_total_size_mb()

    def list_cached_data(self) -> list[dict[str, Any]]:
        return self.storage.list_all()

    def clear_cache(self, symbol: str) -> None:
        self.storage.clear(symbol)

    def clear_all_cache(self) -> None:
        self.storage.clear_all()


@dataclass
class ValidationCheck:
    """Single validation check result."""

    name: str
    passed: bool
    message: str = ""
    details: str = ""


@dataclass
class ValidationResult:
    """Aggregate DataFrame validation result."""

    checks: list[ValidationCheck] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    @property
    def failed_count(self) -> int:
        return sum(1 for c in self.checks if not c.passed)


class DataValidator:
    """DataFrame-oriented data quality validator."""

    def validate(
        self,
        df: pd.DataFrame,
        symbol: str = "",
        verbose: bool = False,
    ) -> ValidationResult:
        result = ValidationResult()
        result.checks.append(self._check_not_empty(df))
        result.checks.append(self._check_columns(df))
        result.checks.append(self._check_nulls(df))
        result.checks.append(self._check_price_sanity(df))
        result.checks.append(self._check_volume_sanity(df))
        result.checks.append(self._check_timestamp_order(df))
        result.checks.append(self._check_gaps(df))
        return result

    def _check_not_empty(self, df: pd.DataFrame) -> ValidationCheck:
        if len(df) == 0:
            return ValidationCheck("Not Empty", False, "DataFrame is empty")
        return ValidationCheck("Not Empty", True, f"{len(df):,} rows")

    def _check_columns(self, df: pd.DataFrame) -> ValidationCheck:
        required = ["timestamp", "open", "high", "low", "close", "volume"]
        missing = [c for c in required if c not in df.columns]
        if missing:
            return ValidationCheck("Required Columns", False, f"Missing: {missing}")
        return ValidationCheck("Required Columns", True, "All present")

    def _check_nulls(self, df: pd.DataFrame) -> ValidationCheck:
        critical = ["timestamp", "open", "high", "low", "close"]
        present = [c for c in critical if c in df.columns]
        if not present:
            return ValidationCheck("No Nulls", False, "No critical columns found")

        null_counts = df[present].isnull().sum()
        total_nulls = int(null_counts.sum())
        if total_nulls > 0:
            details = ", ".join(f"{c}:{n}" for c, n in null_counts.items() if n > 0)
            return ValidationCheck("No Nulls", False, f"{total_nulls} nulls found", details)
        return ValidationCheck("No Nulls", True, "No null values")

    def _check_price_sanity(self, df: pd.DataFrame) -> ValidationCheck:
        if "close" not in df.columns:
            return ValidationCheck("Price Sanity", False, "No close column")

        pct_change = df["close"].pct_change().abs()
        extreme_moves = int((pct_change > 0.10).sum())
        if extreme_moves > 0:
            return ValidationCheck(
                "Price Sanity",
                False,
                f"{extreme_moves} bars with >10% move",
                "May indicate data issues or need for adjustment",
            )
        return ValidationCheck("Price Sanity", True, "All prices within bounds")

    def _check_volume_sanity(self, df: pd.DataFrame) -> ValidationCheck:
        if "volume" not in df.columns:
            return ValidationCheck("Volume Sanity", False, "No volume column")

        zero_volume = int((df["volume"] == 0).sum())
        if zero_volume > 0:
            pct = zero_volume / len(df) * 100
            return ValidationCheck(
                "Volume Sanity",
                True,
                f"{zero_volume} zero-volume bars ({pct:.1f}%)",
                "May be normal for low-volume periods",
            )
        return ValidationCheck("Volume Sanity", True, "All bars have volume")

    def _check_timestamp_order(self, df: pd.DataFrame) -> ValidationCheck:
        if "timestamp" not in df.columns:
            return ValidationCheck("Timestamp Order", False, "No timestamp column")

        timestamps = pd.to_datetime(df["timestamp"])
        out_of_order = int((timestamps.diff().dropna() < pd.Timedelta(0)).sum())
        if out_of_order > 0:
            return ValidationCheck("Timestamp Order", False, f"{out_of_order} out of order")
        return ValidationCheck("Timestamp Order", True, "All timestamps ordered")

    def _check_gaps(self, df: pd.DataFrame) -> ValidationCheck:
        if "timestamp" not in df.columns:
            return ValidationCheck("Gap Detection", False, "No timestamp column")

        timestamps = pd.to_datetime(df["timestamp"])
        gaps = timestamps.diff().dropna()
        large_gaps = gaps[gaps > pd.Timedelta(minutes=5)]
        if len(large_gaps) > 10:
            return ValidationCheck(
                "Gap Detection",
                False,
                f"{len(large_gaps)} gaps > 5 minutes",
                "May indicate missing data",
            )
        return ValidationCheck("Gap Detection", True, "No significant gaps")
