"""Utility functions for the baseline CLI subcommand."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from dbt_bouncer.cli.run.utils import (
    _context_from_config,
    _parse_check_names,
    _parse_only,
    _prepare_bouncer_config,
    _resolve_config_file_source,
)
from dbt_bouncer.cli.utils import resolve_config_path
from dbt_bouncer.reporting.logger import configure_console_logging

if TYPE_CHECKING:
    from pathlib import PurePath

    from dbt_bouncer.enums import ConfigFileSource

DEFAULT_BASELINE_FILE = ".dbt-bouncer-baseline.json"


def write_baseline(
    config_file: PurePath | None = None,
    check: str = "",
    only: str = "",
    output_file: Path | None = None,
    verbosity: int = 0,
    config_file_source: ConfigFileSource | None = None,
) -> int:
    """Run all configured checks and write their failures to a baseline file.

    Args:
        config_file: Location of the config file (YML, YAML, or TOML).
        check: Limit the checks to specific check names, comma-separated.
        only: Limit the checks to specific categories.
        output_file: Where to write the baseline file.
        verbosity: Verbosity level.
        config_file_source: Source of the config file.

    An invalid config propagates as `DbtBouncerConfigError`. A missing dbt
    artifact propagates as `DbtBouncerArtifactError`.

    Returns:
        int: The number of failures written to the baseline.

    """
    import orjson

    from dbt_bouncer.regression import build_baseline
    from dbt_bouncer.runner import collect_failures

    configure_console_logging(verbosity)

    only_parsed = _parse_only(only)
    check_names = _parse_check_names(check)

    config_file = resolve_config_path(config_file)
    config_file_source = _resolve_config_file_source(config_file, config_file_source)

    bouncer_config, check_categories, config_file_path = _prepare_bouncer_config(
        config_file, config_file_source, only_parsed, check_names, preset=None
    )
    ctx = _context_from_config(bouncer_config, check_categories, config_file_path)

    document = build_baseline(collect_failures(ctx))

    target = output_file or Path(DEFAULT_BASELINE_FILE)
    target.write_bytes(orjson.dumps(document, option=orjson.OPT_INDENT_2))
    logging.info(f"Wrote baseline to `{target}`.")

    return len(document["failures"])
