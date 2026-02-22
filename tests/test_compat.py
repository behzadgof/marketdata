"""Tests for compatibility APIs in marketdata.compat."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
import shutil
import uuid

from marketdata.compat import DataConfig, DataManager, DataValidator, ParquetStorage
from marketdata.models.bar import Bar


@dataclass
class _MockProvider:
    bars: list[Bar]

    def get_bars(
        self,
        symbol: str,
        start: date,
        end: date,
        timeframe: str = "1min",
    ) -> list[Bar]:
        return self.bars


class TestParquetStorageCompat:
    def test_save_and_load(self) -> None:
        temp_dir = Path(".tmp_test_cache") / f"storage_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        storage = ParquetStorage(temp_dir)
        import pandas as pd

        df = pd.DataFrame(
            {
                "timestamp": [datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000.0],
            }
        )
        storage.save("AAPL", df, "1min", date(2024, 1, 2), date(2024, 1, 2))
        loaded = storage.load("AAPL", "1min")
        assert len(loaded) == 1
        assert float(loaded.iloc[0]["close"]) == 100.5
        shutil.rmtree(temp_dir, ignore_errors=True)


class TestDataManagerCompat:
    def test_download_historical_uses_provider_and_caches(self) -> None:
        temp_dir = Path(".tmp_test_cache") / f"manager_{uuid.uuid4().hex}"
        temp_dir.mkdir(parents=True, exist_ok=True)
        cfg = DataConfig(cache_path=temp_dir)
        mgr = DataManager(cfg)
        mgr._provider = _MockProvider(
            bars=[
                Bar(
                    timestamp=datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc),
                    open=100.0,
                    high=101.0,
                    low=99.0,
                    close=100.5,
                    volume=1000.0,
                )
            ]
        )

        df = mgr.download_historical("AAPL", date(2024, 1, 2), date(2024, 1, 2))
        assert len(df) == 1
        assert mgr.has_cached_data("AAPL", date(2024, 1, 2), date(2024, 1, 2))
        shutil.rmtree(temp_dir, ignore_errors=True)


class TestDataValidatorCompat:
    def test_validate_passes_on_basic_dataframe(self) -> None:
        import pandas as pd

        df = pd.DataFrame(
            {
                "timestamp": [datetime(2024, 1, 2, 9, 30, tzinfo=timezone.utc)],
                "open": [100.0],
                "high": [101.0],
                "low": [99.0],
                "close": [100.5],
                "volume": [1000.0],
            }
        )

        result = DataValidator().validate(df)
        assert result.passed
