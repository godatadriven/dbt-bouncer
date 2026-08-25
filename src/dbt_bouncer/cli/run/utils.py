"""Utility functions for the run CLI subcommand."""

from __future__ import annotations

import logging
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

from dbt_bouncer.cli.utils import resolve_config_path
from dbt_bouncer.enums import (
    CheckCategory,
    ConfigFileName,
    ConfigFileSource,
    OutputFormat,
)
from dbt_bouncer.exceptions import DbtBouncerConfigError
from dbt_bouncer.reporting.logger import configure_console_logging
from dbt_bouncer.version import version as get_version

if TYPE_CHECKING:
    from collections.abc import Mapping

    from dbt_bouncer.configuration_file.parser import DbtBouncerConfBase
    from dbt_bouncer.context import BouncerContext


def detect_config_file_source(config_file: Path | None) -> ConfigFileSource:
    """Detect the source of the config file.

    Args:
        config_file: Path to the config file, or None for the default.

    Returns:
        ConfigFileSource: 'COMMANDLINE' if a non-default config file was provided, else 'DEFAULT'.

    """
    return (
        ConfigFileSource.COMMANDLINE
        if config_file is not None
        and config_file != Path(ConfigFileName.DBT_BOUNCER_YML)
        and config_file != Path(ConfigFileName.DBT_BOUNCER_YAML)
        and config_file != Path(ConfigFileName.DBT_BOUNCER_TOML)
        else ConfigFileSource.DEFAULT
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


def _configured_categories(config_file_contents: Mapping[str, Any]) -> list[str]:
    """Return the non-empty ``*_checks`` category keys present in the config.

    Returns:
        list[str]: The configured check-category names.

    """
    return [
        i
        for i in config_file_contents
        if i.endswith("_checks") and config_file_contents.get(i) != []
    ]


def _resolve_custom_checks_dir(
    config_file_contents: Mapping[str, Any], config_file_path: PurePath
) -> Path | None:
    """Resolve ``custom_checks_dir`` relative to the config file.

    Returns:
        Path | None: The resolved directory, or ``None`` if not configured.

    """
    custom_checks_dir = config_file_contents.get("custom_checks_dir")
    if not custom_checks_dir:
        return None
    return Path(config_file_path.parent / custom_checks_dir)


def _parse_check_names(check: str) -> set[str]:
    """Parse ``--check`` into a set of check names, rewriting deprecated aliases.

    Rewrite any deprecated name to its replacement and warn once per use.

    Returns:
        set[str]: The requested check names. An empty set means run all checks.

    """
    from dbt_bouncer.configuration_file.validator import (
        DEPRECATED_CHECK_NAME_ALIASES,
        warn_deprecated_check_name,
    )

    check_names: set[str] = set()
    for raw_name in check.strip().split(","):
        name = raw_name.strip()
        if not name:
            continue
        new_name = DEPRECATED_CHECK_NAME_ALIASES.get(name)
        if new_name is not None:
            warn_deprecated_check_name(name, new_name)
            name = new_name
        check_names.add(name)
    return check_names


def _apply_global_severity(config_file_contents: Mapping[str, Any]) -> None:
    """Copy a top-level ``severity`` onto every check entry, in place."""
    severity = config_file_contents.get("severity")
    if not severity:
        return

    logging.info(f"Setting `severity` for all checks to `{severity}`.")
    for category in config_file_contents:
        if category.endswith("_checks") and isinstance(
            config_file_contents[category], list
        ):
            for c in config_file_contents[category]:
                c["severity"] = severity


def _apply_global_check_defaults(
    bouncer_config: DbtBouncerConfBase, check_obj: Any
) -> None:
    """Copy global ``include``/``exclude``/``selector`` onto a check that omits them."""
    if bouncer_config.include and not check_obj.include:
        check_obj.include = bouncer_config.include
    if bouncer_config.exclude and not check_obj.exclude:
        check_obj.exclude = bouncer_config.exclude
    if bouncer_config.selector and not check_obj.selector:
        check_obj.selector = bouncer_config.selector


def _apply_category_filters(
    bouncer_config: DbtBouncerConfBase,
    check_categories: list[str],
    only_parsed: list[str],
) -> None:
    """Index checks, apply global include/exclude/selector, and honour ``--only``.

    Categories not selected by ``--only`` are emptied so no checks run for them.
    """
    for category in check_categories:
        if category not in only_parsed:
            # i.e. if `only` used then remove non-specified check categories
            setattr(bouncer_config, category, [])
            continue
        for idx, check_obj in enumerate(getattr(bouncer_config, category)):
            # Add indices to uniquely identify checks
            check_obj.index = idx
            _apply_global_check_defaults(bouncer_config, check_obj)


def _filter_by_check_names(
    bouncer_config: DbtBouncerConfBase,
    check_categories: list[str],
    check_names: set[str],
) -> None:
    """Restrict each category to checks whose name is in ``check_names``.

    Warn about any requested name that is absent from the (possibly filtered) config.
    """
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
    config_file_source: ConfigFileSource | None = None,
) -> int:
    """Programmatic entrypoint for dbt-bouncer.

    Args:
        config_file: Location of the config file (YML, YAML, or TOML).
        check: Limit the checks run to specific check names, comma-separated.
        create_pr_comment_file: Create a `github-comment.md` file.
        dry_run: If True, print which checks would run without executing them.
        only: Limit the checks run to specific categories.
        output_file: Location of the file where check metadata will be saved.
        output_format: Format for the output file, requires output_file (csv, json, junit, sarif, tap).
        output_only_failures: Only failures will be included in the output file.
        show_all_failures: All failures will be printed to the console.
        verbosity: Verbosity level.
        config_file_source: Source of the config file.

    Returns:
        int: `ExitCode.SUCCESS` if all checks passed, `ExitCode.CHECK_ERRORS` if one
            or more checks failed, `ExitCode.NO_CHECKS_RUN` if the config matched no
            resources and no checks ran.

    Raises:
        DbtBouncerConfigError: If `--only` contains an invalid value, or the config
            file is missing, unreadable, or invalid. A required dbt artifact being
            missing or unsupported similarly propagates as `DbtBouncerArtifactError`
            from the artifact loading called here.
        RuntimeError: If `config_file_source` could not be determined.

    """
    configure_console_logging(verbosity)
    logging.info(f"Running dbt-bouncer ({get_version()})...")

    # Validate `only` has valid values. Raised directly here so this public
    # entrypoint documents the exception it can produce.
    valid_check_categories = [c.value for c in CheckCategory]
    only_parsed = (
        [x.strip() for x in set(only.strip().split(",")) if x != ""]
        if only.strip()
        else valid_check_categories
    )
    invalid = [x for x in only_parsed if x not in valid_check_categories]
    if invalid:
        raise DbtBouncerConfigError(
            f"`--only` contains an invalid value (`{invalid[0]}`). Valid values are `{valid_check_categories}` or any comma-separated combination."
        )

    check_names = _parse_check_names(check)

    # Using local imports to speed up CLI startup
    from dbt_bouncer.configuration_file.validator import (
        get_config_file_path,
        load_config_file_contents,
    )

    config_file = resolve_config_path(config_file)
    if config_file_source is None:
        config_file_source = detect_config_file_source(config_file)

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

    _apply_global_severity(config_file_contents)
    logging.debug(f"{config_file_contents=}")

    check_categories = _configured_categories(config_file_contents)
    logging.debug(f"{check_categories=}")

    custom_checks_dir = _resolve_custom_checks_dir(
        config_file_contents, config_file_path
    )

    from dbt_bouncer.configuration_file.validator import validate_conf

    bouncer_config = validate_conf(
        check_categories=check_categories,
        config_file_contents=dict(config_file_contents),
        custom_checks_dir=custom_checks_dir,
    )
    logging.debug("bouncer_config=%r", bouncer_config)

    _apply_category_filters(bouncer_config, check_categories, only_parsed)

    # Filter to specific check names when `--check` is provided
    if check_names:
        _filter_by_check_names(bouncer_config, check_categories, check_names)

    logging.debug("bouncer_config=%r", bouncer_config)

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
