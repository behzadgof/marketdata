"""Market data error types."""

from __future__ import annotations

from enum import Enum


class MarketDataErrorCode(Enum):
    """Error classification codes."""

    RATE_LIMITED = "rate_limited"
    AUTH_FAILED = "auth_failed"
    NOT_FOUND = "not_found"
    TIMEOUT = "timeout"
    PROVIDER_ERROR = "provider_error"
    VALIDATION_FAILED = "validation_failed"
    NO_DATA = "no_data"


class MarketDataError(Exception):
    """Market data exception with error code and retryable flag.

    Attributes:
        message: Human-readable error description.
        code: Structured error code for programmatic handling.
        retryable: Whether the caller should retry with another provider.
    """

    def __init__(
        self,
        message: str,
        code: MarketDataErrorCode = MarketDataErrorCode.PROVIDER_ERROR,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
