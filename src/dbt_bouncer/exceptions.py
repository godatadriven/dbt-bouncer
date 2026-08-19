"""Typed exceptions used to distinguish CLI failure modes for exit-code mapping."""

from typing import Any


class DbtBouncerConfigError(RuntimeError):
    """Raised when the config file is missing, unreadable, or invalid.

    Args:
        message: Human-readable description of the failure.
        details: Optional structured validation errors, each a mapping with a
            ``message`` and the Pydantic error ``loc`` tuple. Consumers such as
            ``dbt-bouncer validate`` use the ``loc`` to resolve line numbers.

    """

    def __init__(
        self, message: str, details: list[dict[str, Any]] | None = None
    ) -> None:
        """Store the optional structured details alongside the message."""
        super().__init__(message)
        self.details = details


class DbtBouncerArtifactError(RuntimeError):
    """Raised when a required dbt artifact is missing or unsupported."""
