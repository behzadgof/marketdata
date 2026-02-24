"""Provider settings service for configuring market-data backends.

This module is package-owned so multiple applications can reuse the same
provider configuration/update/test behavior without duplicating logic.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from marketdata.config import MarketDataConfig, MarketDataProviderType
from marketdata.manager import MarketDataManager


@dataclass(frozen=True)
class ProviderFieldSpec:
    """Single provider setting field definition."""

    name: str
    env_var: str
    secret: bool


@dataclass(frozen=True)
class ProviderSpec:
    """Provider definition used by settings UI/API."""

    name: str
    fields: tuple[ProviderFieldSpec, ...]


PROVIDER_SPECS: dict[str, ProviderSpec] = {
    "polygon": ProviderSpec(
        name="polygon",
        fields=(ProviderFieldSpec("api_key", "POLYGON_API_KEY", True),),
    ),
    "alpaca": ProviderSpec(
        name="alpaca",
        fields=(
            ProviderFieldSpec("api_key", "ALPACA_API_KEY", True),
            ProviderFieldSpec("api_secret", "ALPACA_SECRET_KEY", True),
        ),
    ),
    "finnhub": ProviderSpec(
        name="finnhub",
        fields=(ProviderFieldSpec("api_key", "FINNHUB_API_KEY", True),),
    ),
    "ib": ProviderSpec(
        name="ib",
        fields=(
            ProviderFieldSpec("host", "IB_HOST", False),
            ProviderFieldSpec("port", "IB_PORT", False),
            ProviderFieldSpec("client_id", "IB_CLIENT_ID", False),
        ),
    ),
    "mock": ProviderSpec(name="mock", fields=()),
}

DEFAULT_PROVIDER_ORDER = ("polygon",)


class ProviderSettingsError(ValueError):
    """Validation error for provider settings operations."""


class MarketDataProviderSettings:
    """Manage market-data provider settings for UI/API endpoints.

    Secrets are stored in environment variables and optionally persisted to
    ``.env``. Non-secret metadata is stored in a JSON state file.

    By default, settings are rooted at the current working directory unless an
    explicit ``app_root`` or explicit file paths are provided.
    """

    def __init__(
        self,
        env_path: Path | str | None = None,
        state_path: Path | str | None = None,
        app_root: Path | str | None = None,
    ) -> None:
        root = Path(app_root).resolve() if app_root else Path.cwd()
        self._env_path = Path(env_path) if env_path else root / ".env"
        self._state_path = (
            Path(state_path)
            if state_path
            else root / "state" / "marketdata_provider_settings.json"
        )
        self._state_path.parent.mkdir(parents=True, exist_ok=True)

    def list_providers(self) -> dict[str, Any]:
        """Return provider metadata with masked values."""
        env_values = self._combined_env()
        state = self._load_state()
        order = self._provider_order_from_env(env_values)

        providers: list[dict[str, Any]] = []
        for name, spec in PROVIDER_SPECS.items():
            status = state.get("providers", {}).get(name, {})
            enabled = name in order
            priority = order.index(name) + 1 if enabled else None

            fields = []
            for field in spec.fields:
                raw = env_values.get(field.env_var, "")
                configured = bool(raw)
                display = self._mask_value(raw) if field.secret else (raw or "")
                fields.append(
                    {
                        "name": field.name,
                        "env_var": field.env_var,
                        "secret": field.secret,
                        "configured": configured,
                        "value": display,
                    }
                )

            providers.append(
                {
                    "provider": name,
                    "enabled": enabled,
                    "priority": priority,
                    "fields": fields,
                    "last_test_status": status.get("last_test_status"),
                    "last_test_message": status.get("last_test_message"),
                    "last_tested_at": status.get("last_tested_at"),
                    "updated_at": status.get("updated_at"),
                }
            )

        providers.sort(
            key=lambda p: (p["priority"] is None, p["priority"] or 999, p["provider"])
        )
        return {"providers": providers, "active_provider_order": order}

    def update_provider(self, provider: str, payload: dict[str, Any]) -> dict[str, Any]:
        """Update provider settings.

        Payload shape:
            {
              "enabled": bool,
              "priority": int,  # 1-based
              "values": {"api_key": "..."},
              "clear": ["api_key"],
              "persist": true
            }
        """
        provider_key = self._normalize_provider(provider)
        spec = PROVIDER_SPECS[provider_key]

        persist = bool(payload.get("persist", True))
        enabled = payload.get("enabled")
        priority = payload.get("priority")
        values = payload.get("values", {}) or {}
        clear = payload.get("clear", []) or []

        if enabled is not None and not isinstance(enabled, bool):
            raise ProviderSettingsError("'enabled' must be boolean when provided")
        if priority is not None:
            if not isinstance(priority, int) or priority < 1:
                raise ProviderSettingsError("'priority' must be an integer >= 1")
        if not isinstance(values, dict):
            raise ProviderSettingsError("'values' must be an object")
        if not isinstance(clear, list) or not all(isinstance(x, str) for x in clear):
            raise ProviderSettingsError("'clear' must be an array of field names")

        env_updates: dict[str, str | None] = {}
        allowed_names = {f.name for f in spec.fields}
        for field_name, value in values.items():
            if field_name not in allowed_names:
                raise ProviderSettingsError(
                    f"Unsupported field '{field_name}' for provider '{provider_key}'"
                )
            env_var = next(f.env_var for f in spec.fields if f.name == field_name)
            env_updates[env_var] = str(value)

        for field_name in clear:
            if field_name not in allowed_names:
                raise ProviderSettingsError(
                    f"Unsupported field '{field_name}' for provider '{provider_key}'"
                )
            env_var = next(f.env_var for f in spec.fields if f.name == field_name)
            env_updates[env_var] = None

        if env_updates:
            self._apply_env_updates(env_updates, persist=persist)

        env_values = self._combined_env()
        order = self._provider_order_from_env(env_values)
        order = self._apply_order_update(
            order=order,
            provider=provider_key,
            enabled=enabled,
            priority=priority,
        )
        self._set_provider_order(order, persist=persist)

        state = self._load_state()
        provider_state = state.setdefault("providers", {}).setdefault(provider_key, {})
        provider_state["updated_at"] = date.today().isoformat()
        self._save_state(state)

        return self._provider_snapshot(provider_key)

    def test_provider(
        self,
        provider: str,
        symbol: str = "AAPL",
    ) -> dict[str, Any]:
        """Test provider connectivity by fetching recent daily bars."""
        provider_key = self._normalize_provider(provider)

        env_values = self._combined_env()
        config = MarketDataConfig(
            providers=[MarketDataProviderType(provider_key)],
            cache_backend="none",
            validate=True,
            polygon_api_key=env_values.get("POLYGON_API_KEY"),
            alpaca_api_key=env_values.get("ALPACA_API_KEY"),
            alpaca_api_secret=env_values.get("ALPACA_SECRET_KEY"),
            finnhub_api_key=env_values.get("FINNHUB_API_KEY"),
            ib_host=env_values.get("IB_HOST", "127.0.0.1"),
            ib_port=int(env_values.get("IB_PORT", "7497")),
            ib_client_id=int(env_values.get("IB_CLIENT_ID", "1")),
        )

        manager = MarketDataManager(config)
        end = date.today() - timedelta(days=1)
        start = end - timedelta(days=5)

        state = self._load_state()
        provider_state = state.setdefault("providers", {}).setdefault(provider_key, {})
        try:
            bars = manager.get_bars(symbol=symbol, start=start, end=end, timeframe="1day")
            result = {
                "ok": True,
                "provider": provider_key,
                "symbol": symbol.upper(),
                "bars": len(bars),
                "start": start.isoformat(),
                "end": end.isoformat(),
                "message": f"Fetched {len(bars)} bars",
            }
            provider_state["last_test_status"] = "passed"
            provider_state["last_test_message"] = result["message"]
        except Exception as exc:  # noqa: BLE001
            result = {
                "ok": False,
                "provider": provider_key,
                "symbol": symbol.upper(),
                "bars": 0,
                "start": start.isoformat(),
                "end": end.isoformat(),
                "message": str(exc),
            }
            provider_state["last_test_status"] = "failed"
            provider_state["last_test_message"] = str(exc)

        provider_state["last_tested_at"] = date.today().isoformat()
        self._save_state(state)
        return result

    def _provider_snapshot(self, provider: str) -> dict[str, Any]:
        data = self.list_providers()
        for entry in data["providers"]:
            if entry["provider"] == provider:
                return entry
        raise ProviderSettingsError(f"Provider '{provider}' not found")

    def _normalize_provider(self, provider: str) -> str:
        provider_key = provider.strip().lower()
        if provider_key not in PROVIDER_SPECS:
            raise ProviderSettingsError(
                f"Unsupported provider '{provider}'. "
                f"Supported: {', '.join(PROVIDER_SPECS.keys())}"
            )
        return provider_key

    @staticmethod
    def _mask_value(value: str) -> str:
        if not value:
            return ""
        if len(value) <= 4:
            return "*" * len(value)
        return f"{value[:2]}{'*' * (len(value) - 4)}{value[-2:]}"

    def _combined_env(self) -> dict[str, str]:
        values = self._read_env_file()
        for key, value in os.environ.items():
            if value:
                values[key] = value
        return values

    def _provider_order_from_env(self, env_values: dict[str, str]) -> list[str]:
        raw = env_values.get("MARKET_DATA_PROVIDERS", "")
        if raw.strip():
            order = []
            for name in raw.split(","):
                key = name.strip().lower()
                if key in PROVIDER_SPECS and key not in order:
                    order.append(key)
            if order:
                return order
        return list(DEFAULT_PROVIDER_ORDER)

    def _set_provider_order(self, order: list[str], persist: bool) -> None:
        joined = ",".join(order)
        self._apply_env_updates({"MARKET_DATA_PROVIDERS": joined}, persist=persist)

    @staticmethod
    def _apply_order_update(
        order: list[str],
        provider: str,
        enabled: bool | None,
        priority: int | None,
    ) -> list[str]:
        updated = list(order)
        if enabled is True and provider not in updated:
            updated.append(provider)
        if enabled is False and provider in updated:
            updated.remove(provider)

        if priority is not None and provider in updated:
            updated.remove(provider)
            idx = min(max(priority - 1, 0), len(updated))
            updated.insert(idx, provider)

        if not updated:
            updated = list(DEFAULT_PROVIDER_ORDER)
        return updated

    def _read_env_file(self) -> dict[str, str]:
        if not self._env_path.exists():
            return {}
        values: dict[str, str] = {}
        for raw_line in self._env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if line.startswith("export "):
                line = line[len("export ") :].strip()
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if value.startswith(("'", '"')) and value.endswith(("'", '"')):
                value = value[1:-1]
            values[key] = value
        return values

    def _apply_env_updates(self, updates: dict[str, str | None], persist: bool) -> None:
        for key, value in updates.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value
        if persist:
            self._write_env_file(updates)

    def _write_env_file(self, updates: dict[str, str | None]) -> None:
        lines: list[str] = []
        if self._env_path.exists():
            lines = self._env_path.read_text(encoding="utf-8").splitlines()

        remaining = dict(updates)
        output: list[str] = []
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                output.append(line)
                continue
            key = stripped.split("=", 1)[0].strip()
            if key not in remaining:
                output.append(line)
                continue
            value = remaining.pop(key)
            if value is None:
                continue
            output.append(f"{key}={value}")

        for key, value in remaining.items():
            if value is None:
                continue
            output.append(f"{key}={value}")

        self._env_path.parent.mkdir(parents=True, exist_ok=True)
        self._env_path.write_text("\n".join(output) + "\n", encoding="utf-8")

    def _load_state(self) -> dict[str, Any]:
        if not self._state_path.exists():
            return {"providers": {}}
        try:
            raw = json.loads(self._state_path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
        except Exception:  # noqa: BLE001
            pass
        return {"providers": {}}

    def _save_state(self, state: dict[str, Any]) -> None:
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._state_path.write_text(
            json.dumps(state, indent=2, sort_keys=True),
            encoding="utf-8",
        )


__all__ = [
    "ProviderFieldSpec",
    "ProviderSpec",
    "ProviderSettingsError",
    "MarketDataProviderSettings",
    "PROVIDER_SPECS",
    "DEFAULT_PROVIDER_ORDER",
]
