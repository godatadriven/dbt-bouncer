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
    PresetName,
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


def _resolve_config_contents(
    config_file: PurePath,
    config_file_source: ConfigFileSource,
    preset: PresetName | None,
) -> tuple[dict[str, Any], PurePath]:
    """Load the config contents, from a bundled preset or a config file.

    A preset is used only when requested and no explicit `--config-file` was
    given. When a preset is used, paths resolve relative to the current working
    directory.

    Returns:
        tuple[dict[str, Any], PurePath]: The config contents and the config file
            path used to resolve relative paths.

    """
    from dbt_bouncer.configuration_file.validator import (
        get_config_file_path,
        load_config_file_contents,
    )

    if preset is not None and config_file_source == ConfigFileSource.COMMANDLINE:
        logging.warning(
            f"Both `--preset` and an explicit `--config-file` were provided. Ignoring `--preset {preset}` and using the config file."
        )
        preset = None

    if preset is not None:
        from dbt_bouncer.presets import load_preset_contents

        logging.info(f"Using the `{preset}` preset configuration.")
        config_file_path = Path.cwd() / ConfigFileName.DBT_BOUNCER_YML
        return load_preset_contents(preset), config_file_path

    config_file_path = get_config_file_path(
        config_file=config_file,
        config_file_source=config_file_source,
    )
    config_file_contents = load_config_file_contents(
        config_file_path, allow_default_config_file_creation=True
    )
    return dict(config_file_contents), config_file_path


def _parse_only(only: str) -> list[str]:
    """Parse and validate ``--only`` into a list of category names.

    Returns:
        list[str]: The requested categories, or all categories when empty.

    Raises:
        DbtBouncerConfigError: If a value is not a valid check category.

    """
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
    return only_parsed


def _prepare_bouncer_config(
    config_file: PurePath,
    config_file_source: ConfigFileSource,
    only_parsed: list[str],
    check_names: set[str],
    preset: PresetName | None,
) -> tuple[DbtBouncerConfBase, list[str], PurePath]:
    """Load and filter the config, ready to build a context.

    Uses a bundled preset when requested, else the config file.

    Returns:
        tuple[DbtBouncerConfBase, list[str], PurePath]: The validated config, the
            configured categories, and the config file path.

    """
    from dbt_bouncer.configuration_file.validator import validate_conf

    config_file_contents, config_file_path = _resolve_config_contents(
        config_file, config_file_source, preset
    )

    _apply_global_severity(config_file_contents)
    check_categories = _configured_categories(config_file_contents)
    custom_checks_dir = _resolve_custom_checks_dir(
        config_file_contents, config_file_path
    )

    bouncer_config = validate_conf(
        check_categories=check_categories,
        config_file_contents=dict(config_file_contents),
        custom_checks_dir=custom_checks_dir,
    )

    _apply_category_filters(bouncer_config, check_categories, only_parsed)
    if check_names:
        _filter_by_check_names(bouncer_config, check_categories, check_names)

    return bouncer_config, check_categories, config_file_path


def _artifacts_dir(
    bouncer_config: DbtBouncerConfBase, config_file_path: PurePath
) -> Path:
    """Resolve the dbt artifacts directory for a config.

    Returns:
        Path: The artifacts directory.

    """
    return Path(
        config_file_path.parent / (bouncer_config.dbt_artifacts_dir or "target")
    )


def _context_from_config(
    bouncer_config: DbtBouncerConfBase,
    check_categories: list[str],
    config_file_path: PurePath,
    *,
    create_pr_comment_file: bool = False,
    dry_run: bool = False,
    output_file: Path | None = None,
    output_format: OutputFormat = OutputFormat.JSON,
    output_only_failures: bool = False,
    show_all_failures: bool = False,
    dbt_artifacts_dir: Path | None = None,
) -> BouncerContext:
    """Parse artifacts and build a context from a prepared config.

    Returns:
        BouncerContext: Ready-to-run context.

    """
    normalized_output_format = (
        output_format.value
        if isinstance(output_format, OutputFormat)
        else OutputFormat(output_format.lower()).value
    )
    return _build_context(
        bouncer_config=bouncer_config,
        check_categories=check_categories,
        create_pr_comment_file=create_pr_comment_file,
        dbt_artifacts_dir=dbt_artifacts_dir
        or _artifacts_dir(bouncer_config, config_file_path),
        dry_run=dry_run,
        output_file=output_file,
        output_format=normalized_output_format,
        output_only_failures=output_only_failures,
        show_all_failures=show_all_failures,
    )


def _resolve_accepted_fingerprints(
    baseline: Path | None,
    state: Path | None,
    bouncer_config: DbtBouncerConfBase,
    check_categories: list[str],
    config_file_path: PurePath,
) -> set[str] | None:
    """Build the set of already-known failure fingerprints to suppress.

    Combines a stored baseline file and a `--state` base run. Returns ``None``
    when neither is requested so the normal run is unchanged.

    Returns:
        set[str] | None: The accepted fingerprints, or ``None``.

    Raises:
        DbtBouncerConfigError: If `--state` is not a directory.
        DbtBouncerArtifactError: If the base artifacts cannot be loaded.

    """
    if baseline is None and state is None:
        return None

    from dbt_bouncer.regression import failure_fingerprints, load_baseline

    accepted: set[str] = set()
    if baseline is not None:
        accepted |= load_baseline(baseline)

    if state is not None:
        from dbt_bouncer.exceptions import DbtBouncerArtifactError
        from dbt_bouncer.runner import collect_failures

        if not state.is_dir():
            raise DbtBouncerConfigError(
                f"`--state {state}` is not a directory. Point it at a directory of dbt artifacts from a previous run (the `target` directory containing `manifest.json`)."
            )
        logging.info(f"Running checks against base artifacts in `{state}`...")
        try:
            base_ctx = _context_from_config(
                bouncer_config,
                check_categories,
                config_file_path,
                dbt_artifacts_dir=state,
            )
            base_failures = collect_failures(base_ctx)
        except DbtBouncerArtifactError as e:
            # Name `--state` as the source so the user knows which artifact set
            # failed to load (the base one, not the current run's).
            raise DbtBouncerArtifactError(
                f"Failed to load the base artifacts from `--state {state}`: {e}"
            ) from e
        accepted |= failure_fingerprints(base_failures)

    return accepted


def _resolve_config_file_source(
    config_file: PurePath, config_file_source: ConfigFileSource | None
) -> ConfigFileSource:
    """Resolve the config file source, detecting it when not given.

    Returns:
        ConfigFileSource: The resolved source.

    Raises:
        RuntimeError: If the source could not be determined.

    """
    if config_file_source is None:
        config_file_source = detect_config_file_source(Path(config_file))
    if (
        config_file_source is None
    ):  # pragma: no cover — unreachable; narrows type for the checker.
        raise RuntimeError(
            "config_file_source was not set by the config-file lookup logic."
        )
    return config_file_source


def run_bouncer(
    config_file: PurePath | None = None,
    check: str = "",
    create_pr_comment_file: bool = False,
    dry_run: bool = False,
    only: str = "",
    output_file: Path | None = None,
    output_format: OutputFormat = OutputFormat.JSON,
    output_only_failures: bool = False,
    preset: PresetName | None = None,
    show_all_failures: bool = False,
    verbosity: int = 0,
    config_file_source: ConfigFileSource | None = None,
    baseline: Path | None = None,
    state: Path | None = None,
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
        preset: Use a bundled preset config (minimal, standard, strict) instead of a config file. Ignored when an explicit `--config-file` is provided.
        show_all_failures: All failures will be printed to the console.
        verbosity: Verbosity level.
        config_file_source: Source of the config file.
        baseline: Path to a baseline file. Failures listed in it are suppressed.
        state: Directory of dbt artifacts from a previous run. Failures present in that base run are suppressed.

    An invalid `--only` value, a missing or invalid config file, or a missing
    baseline file propagate as `DbtBouncerConfigError`. A missing or unsupported
    dbt artifact propagates as `DbtBouncerArtifactError`.

    Returns:
        int: `ExitCode.SUCCESS` if all checks passed, `ExitCode.CHECK_ERRORS` if one
            or more checks failed, `ExitCode.NO_CHECKS_RUN` if the config matched no
            resources and no checks ran.

    """
    configure_console_logging(verbosity)
    logging.info(f"Running dbt-bouncer ({get_version()})...")

    only_parsed = _parse_only(only)
    check_names = _parse_check_names(check)

    config_file = resolve_config_path(config_file)
    config_file_source = _resolve_config_file_source(config_file, config_file_source)

    bouncer_config, check_categories, config_file_path = _prepare_bouncer_config(
        config_file, config_file_source, only_parsed, check_names, preset
    )

    accept = _resolve_accepted_fingerprints(
        baseline, state, bouncer_config, check_categories, config_file_path
    )

    ctx = _context_from_config(
        bouncer_config,
        check_categories,
        config_file_path,
        create_pr_comment_file=create_pr_comment_file,
        dry_run=dry_run,
        output_file=output_file,
        output_format=output_format,
        output_only_failures=output_only_failures,
        show_all_failures=show_all_failures,
    )

    from dbt_bouncer.runner import runner

    results = runner(ctx=ctx, accept=accept)
    return results[0]
