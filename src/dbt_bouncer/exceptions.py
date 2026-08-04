"""Typed exceptions used to distinguish CLI failure modes for exit-code mapping."""


class DbtBouncerConfigError(RuntimeError):
    """Raised when the config file is missing, unreadable, or invalid."""


class DbtBouncerArtifactError(RuntimeError):
    """Raised when a required dbt artifact is missing or unsupported."""
