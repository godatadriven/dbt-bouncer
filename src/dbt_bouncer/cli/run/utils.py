"""Utility functions for the run CLI subcommand."""

from __future__ import annotations

import logging
from pathlib import Path, PurePath
from typing import TYPE_CHECKING

from dbt_bouncer.enums import ConfigFileName, OutputFormat
from dbt_bouncer.reporting.logger import configure_console_logging
from dbt_bouncer.version import version as get_version

if TYPE_CHECKING:
    from dbt_bouncer.config_file_parser import DbtBouncerConfBase
    from dbt_bouncer.context import BouncerContext


def _detect_config_file_source(config_file: Path | None) -> str:
    """Detect the source of the config file.

    Args:
        config_file: Path to the config file, or None for the default.

    Returns:
        str: 'COMMANDLINE' if a non-default config file was provided, else 'DEFAULT'.

    """
    return (
        "COMMANDLINE"
        if config_file is not None
        and config_file != Path(ConfigFileName.DBT_BOUNCER_YML)
        and config_file != Path(ConfigFileName.DBT_BOUNCER_TOML)
        else "DEFAULT"
    )


def _build_context(
    bouncer_config: DbtBouncerConfBase,
    check_categories: list[str],
    create_pr_comment_file: bool,
    dbt_artifacts_dir: Path,
    output_file: Path | None,
    output_format: str,
    output_only_failures: bool,
    dry_run: bool = False,
    show_all_failures: bool = False,
) -> BouncerContext:
    """Parse artifacts and build a BouncerContext.

    Returns:
        BouncerContext: Ready-to-run context.

    """
    from dbt_bouncer.artifact_parsers.parser import parse_dbt_artifacts
    from dbt_bouncer.context import BouncerContext

    artifacts = parse_dbt_artifacts(
        bouncer_config=bouncer_config, dbt_artifacts_dir=dbt_artifacts_dir
    )

    return BouncerContext.model_construct(
        bouncer_config=bouncer_config,
        catalog_nodes=artifacts.catalog_nodes,
        catalog_sources=artifacts.catalog_sources,
        check_categories=check_categories,
        create_pr_comment_file=create_pr_comment_file,
        dry_run=dry_run,
        exposures=artifacts.exposures,
        macros=artifacts.macros,
        manifest_obj=artifacts.manifest_obj,
        models=artifacts.models,
        output_file=output_file,
        output_format=output_format,
        output_only_failures=output_only_failures,
        run_results=artifacts.run_results,
        seeds=artifacts.seeds,
        semantic_models=artifacts.semantic_models,
        show_all_failures=show_all_failures,
        snapshots=artifacts.snapshots,
        sources=artifacts.sources,
        tests=artifacts.tests,
        unit_tests=artifacts.unit_tests,
    )


def run_bouncer(
    config_file: PurePath | None = None,
    check: str = "",
    create_pr_comment_file: bool = False,
    dry_run: bool = False,
    only: str = "",
    output_file: Path | None = None,
    output_format: OutputFormat = OutputFormat.JSON,
    output_only_failures: bool = False,
    show_all_failures: bool = False,
    verbosity: int = 0,
    config_file_source: str | None = None,
) -> int:
    """Programmatic entrypoint for dbt-bouncer.

    Args:
        config_file: Location of the config file (YML, YAML, or TOML).
        check: Limit the checks run to specific check names, comma-separated.
        create_pr_comment_file: Create a `github-comment.md` file.
        dry_run: If True, print which checks would run without executing them.
        only: Limit the checks run to specific categories.
        output_file: Location of the file where check metadata will be saved.
        output_format: Format for the output file or stdout (csv, json, junit, sarif, tap).
        output_only_failures: Only failures will be included in the output file.
        show_all_failures: All failures will be printed to the console.
        verbosity: Verbosity level.
        config_file_source: Source of the config file ("COMMANDLINE", "DEFAULT", etc.).

    Returns:
        int: Exit code (0 for success, 1 for failure).

    Raises:
        RuntimeError: If runtime errors occur.

    """
    configure_console_logging(verbosity)
    logging.info(f"Running dbt-bouncer ({get_version()})...")

    # Validate `only` has valid values
    valid_check_categories = ["catalog_checks", "manifest_checks", "run_results_checks"]
    if not only.strip():
        only_parsed = valid_check_categories
    else:
        only_parsed = [x.strip() for x in set(only.strip().split(",")) if x != ""]

    for x in [i for i in only_parsed if i not in valid_check_categories]:
        raise RuntimeError(
            f"`--only` contains an invalid value (`{x}`). Valid values are `{valid_check_categories}` or any comma-separated combination."
        )

    # Parse `--check` into a set of check names (empty set means run all)
    check_names: set[str] = {x.strip() for x in check.strip().split(",") if x.strip()}

    # Using local imports to speed up CLI startup
    from dbt_bouncer.config_file_validator import (
        get_config_file_path,
        load_config_file_contents,
    )

    if config_file is None:
        config_file = Path(ConfigFileName.DBT_BOUNCER_YML)
        if config_file_source is None:
            config_file_source = "DEFAULT"
    else:
        if config_file_source is None:
            config_file_source = "COMMANDLINE"

    if (
        config_file_source is None
    ):  # pragma: no cover — unreachable; narrows type for the checker.
        raise RuntimeError(
            "config_file_source was not set by the config-file lookup logic."
        )
    config_file_path = get_config_file_path(
        config_file=config_file,
        config_file_source=config_file_source,
    )
    config_file_contents = load_config_file_contents(
        config_file_path, allow_default_config_file_creation=True
    )

    # Handle `severity` at the global level
    if config_file_contents.get("severity"):
        logging.info(
            f"Setting `severity` for all checks to `{config_file_contents['severity']}`."
        )
        for category in config_file_contents:
            if category.endswith("_checks") and isinstance(
                config_file_contents[category], list
            ):
                for c in config_file_contents[category]:
                    c["severity"] = config_file_contents["severity"]

    logging.debug(f"{config_file_contents=}")

    check_categories = [
        i
        for i in config_file_contents
        if i.endswith("_checks") and config_file_contents.get(i) != []
    ]
    logging.debug(f"{check_categories=}")

    # Resolve custom_checks_dir relative to config file
    custom_checks_dir = None
    if config_file_contents.get("custom_checks_dir"):
        custom_checks_dir = (
            config_file_path.parent / config_file_contents["custom_checks_dir"]
        )

    from dbt_bouncer.config_file_validator import validate_conf

    bouncer_config = validate_conf(
        check_categories=check_categories,
        config_file_contents=dict(config_file_contents),
        custom_checks_dir=Path(custom_checks_dir) if custom_checks_dir else None,
    )
    logging.debug(f"{bouncer_config=}")

    for category in check_categories:
        if category in only_parsed:
            for idx, check_obj in enumerate(getattr(bouncer_config, category)):
                # Add indices to uniquely identify checks
                check_obj.index = idx

                # Handle global `exclude` and `include` args
                if bouncer_config.include and not check_obj.include:
                    check_obj.include = bouncer_config.include
                if bouncer_config.exclude and not check_obj.exclude:
                    check_obj.exclude = bouncer_config.exclude
        else:
            # i.e. if `only` used then remove non-specified check categories
            setattr(bouncer_config, category, [])

    # Filter to specific check names when `--check` is provided
    if check_names:
        all_configured_names: set[str] = {
            c.name
            for category in check_categories
            for c in getattr(bouncer_config, category)
        }
        unknown_names = check_names - all_configured_names
        if unknown_names:
            logging.warning(
                f"`--check` contains values not found in the (possibly filtered) config: {sorted(unknown_names)}. No checks will run for these names."
            )

        for category in check_categories:
            setattr(
                bouncer_config,
                category,
                [c for c in getattr(bouncer_config, category) if c.name in check_names],
            )

    logging.debug(f"{bouncer_config=}")

    dbt_artifacts_dir = Path(
        config_file_path.parent / (bouncer_config.dbt_artifacts_dir or "target")
    )

    from dbt_bouncer.runner import runner

    normalized_output_format = (
        output_format.value
        if isinstance(output_format, OutputFormat)
        else OutputFormat(output_format.lower()).value
    )

    ctx = _build_context(
        bouncer_config=bouncer_config,
        check_categories=check_categories,
        create_pr_comment_file=create_pr_comment_file,
        dbt_artifacts_dir=dbt_artifacts_dir,
        dry_run=dry_run,
        output_file=output_file,
        output_format=normalized_output_format,
        output_only_failures=output_only_failures,
        show_all_failures=show_all_failures,
    )
    results = runner(ctx=ctx)
    return results[0]
