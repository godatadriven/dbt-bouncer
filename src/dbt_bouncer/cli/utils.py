"""Shared utility functions for CLI subcommands."""

from __future__ import annotations

from pathlib import Path

from dbt_bouncer.enums import ConfigFileName


def resolve_config_path(config_file: Path | None) -> Path:
    """Resolve the config file path, defaulting to ``dbt-bouncer.yml``.

    Args:
        config_file: Explicit path, or None to use the default.

    Returns:
        Path: The resolved config file path.

    """
    return (
        Path(ConfigFileName.DBT_BOUNCER_YML)
        if config_file is None
        else Path(config_file)
    )
