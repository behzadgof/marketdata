"""Tests for market-data provider settings service."""

from __future__ import annotations

from pathlib import Path

import pytest

from marketdata.provider_settings import (
    MarketDataProviderSettings,
    ProviderSettingsError,
)


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch) -> None:
    keys = [
        "MARKET_DATA_PROVIDERS",
        "POLYGON_API_KEY",
        "ALPACA_API_KEY",
        "ALPACA_SECRET_KEY",
        "FINNHUB_API_KEY",
        "IB_HOST",
        "IB_PORT",
        "IB_CLIENT_ID",
    ]
    for key in keys:
        monkeypatch.delenv(key, raising=False)


def _service(tmp_path: Path) -> MarketDataProviderSettings:
    env_path = tmp_path / ".env"
    state_path = tmp_path / "state" / "provider_settings.json"
    return MarketDataProviderSettings(env_path=env_path, state_path=state_path)


def test_list_providers_defaults(tmp_path) -> None:
    svc = _service(tmp_path)
    data = svc.list_providers()
    assert "providers" in data
    assert data["active_provider_order"] == ["polygon"]
    polygon = next(p for p in data["providers"] if p["provider"] == "polygon")
    assert polygon["enabled"] is True
    assert polygon["fields"][0]["configured"] is False


def test_update_polygon_persists_env_and_masks(tmp_path) -> None:
    svc = _service(tmp_path)
    result = svc.update_provider(
        "polygon",
        {
            "enabled": True,
            "priority": 1,
            "values": {"api_key": "pk_test_123456"},
            "persist": True,
        },
    )
    assert result["provider"] == "polygon"
    assert result["fields"][0]["configured"] is True
    assert "*" in result["fields"][0]["value"]

    env_text = (tmp_path / ".env").read_text(encoding="utf-8")
    assert "POLYGON_API_KEY=pk_test_123456" in env_text
    assert "MARKET_DATA_PROVIDERS=polygon" in env_text


def test_update_provider_priority_updates_order(tmp_path) -> None:
    svc = _service(tmp_path)
    svc.update_provider(
        "polygon",
        {"enabled": True, "priority": 2, "values": {"api_key": "pk_test"}, "persist": True},
    )
    svc.update_provider(
        "alpaca",
        {
            "enabled": True,
            "priority": 1,
            "values": {"api_key": "ak", "api_secret": "sk"},
            "persist": True,
        },
    )
    order = svc.list_providers()["active_provider_order"]
    assert order[:2] == ["alpaca", "polygon"]


def test_clear_provider_secret(tmp_path) -> None:
    svc = _service(tmp_path)
    svc.update_provider(
        "polygon",
        {"values": {"api_key": "pk_test_123456"}, "persist": True},
    )
    svc.update_provider(
        "polygon",
        {"clear": ["api_key"], "persist": True},
    )
    polygon = next(
        p for p in svc.list_providers()["providers"] if p["provider"] == "polygon"
    )
    assert polygon["fields"][0]["configured"] is False


def test_invalid_provider_raises(tmp_path) -> None:
    svc = _service(tmp_path)
    with pytest.raises(ProviderSettingsError):
        svc.update_provider("unknown", {"enabled": True})


def test_test_provider_mock_passes(tmp_path) -> None:
    svc = _service(tmp_path)
    svc.update_provider("mock", {"enabled": True, "persist": False})
    result = svc.test_provider("mock", symbol="AAPL")
    assert result["provider"] == "mock"
    assert result["ok"] is True
    assert result["bars"] > 0
