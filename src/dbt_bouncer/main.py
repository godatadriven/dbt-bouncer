import logging
from pathlib import Path, PurePath

import click

from dbt_bouncer.logger import configure_console_logging
from dbt_bouncer.version import version


def run_bouncer(
    config_file: PurePath | None = None,
    check: str = "",
    create_pr_comment_file: bool = False,
    only: str = "",
    output_file: Path | None = None,
    output_format: str = "json",
    output_only_failures: bool = False,
    show_all_failures: bool = False,
    verbosity: int = 0,
    config_file_source: str | None = None,
) -> int:
    """Programmatic entrypoint for dbt-bouncer.

    Args:
        config_file: Location of the YML config file.
        check: Limit the checks run to specific check names, comma-separated.
        create_pr_comment_file: Create a `github-comment.md` file.
        only: Limit the checks run to specific categories.
        output_file: Location of the file where check metadata will be saved.
        output_format: Format for the output file or stdout (json, junit).
        output_only_failures: Only failures will be included in the output file.
        show_all_failures: All failures will be printed to the console.
        verbosity: Verbosity level.
        config_file_source: Source of the config file ("COMMANDLINE", "DEFAULT", etc.).

    Returns:
        int: Exit code (0 for success, 1 for failure).

    Raises:
        AssertionError: If config_file_source is None.
        RuntimeError: If other runtime errors occur.

    """
    configure_console_logging(verbosity)
    logging.info(f"Running dbt-bouncer ({version()})...")

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
        config_file = Path("dbt-bouncer.yml")
        if config_file_source is None:
            config_file_source = "DEFAULT"
    else:
        if config_file_source is None:
            config_file_source = "COMMANDLINE"

    if config_file_source is None:
        raise AssertionError("config_file_source cannot be None")

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
        for c in config_file_contents["manifest_checks"]:
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
    del config_file_contents
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

    from dbt_bouncer.artifact_parsers.parsers_common import parse_dbt_artifacts

    (
        manifest_obj,
        project_exposures,
        project_macros,
        project_models,
        project_seeds,
        project_semantic_models,
        project_snapshots,
        project_sources,
        project_tests,
        project_unit_tests,
        project_catalog_nodes,
        project_catalog_sources,
        project_run_results,
    ) = parse_dbt_artifacts(
        bouncer_config=bouncer_config, dbt_artifacts_dir=dbt_artifacts_dir
    )

    logging.info("Running checks...")
    from dbt_bouncer.runner import runner

    results = runner(
        bouncer_config=bouncer_config,
        catalog_nodes=project_catalog_nodes,
        catalog_sources=project_catalog_sources,
        check_categories=check_categories,
        create_pr_comment_file=create_pr_comment_file,
        exposures=project_exposures,
        macros=project_macros,
        manifest_obj=manifest_obj,
        models=project_models,
        output_file=output_file,
        output_format=output_format,
        output_only_failures=output_only_failures,
        run_results=project_run_results,
        seeds=project_seeds,
        semantic_models=project_semantic_models,
        show_all_failures=show_all_failures,
        snapshots=project_snapshots,
        sources=project_sources,
        tests=project_tests,
        unit_tests=project_unit_tests,
    )
    return results[0]


@click.group(invoke_without_command=True)
@click.option(
    "--config-file",
    default=Path("dbt-bouncer.yml"),
    help="Location of the YML config file.",
    required=False,
    type=PurePath,
)
@click.option(
    "--create-pr-comment-file",
    default=False,
    help="Create a `github-comment.md` file that will be sent to GitHub as a PR comment. Defaults to True when `dbt-bouncer` is run as a GitHub Action.",
    hidden=True,
    required=False,
    type=click.BOOL,
)
@click.option(
    "--check",
    default="",
    help="Limit the checks run to specific check names, comma-separated. Examples: 'check_model_has_unique_test', 'check_model_names,check_source_freshness_populated'.",
    required=False,
    type=str,
)
@click.option(
    "--only",
    default="",
    help="Limit the checks run to specific categories, comma-separated. Examples: 'manifest_checks', 'catalog_checks,manifest_checks'.",
    required=False,
    type=str,
)
@click.option(
    "--output-file",
    default=None,
    help="Location of the file where check metadata will be saved.",
    required=False,
    type=Path,
)
@click.option(
    "--output-format",
    default="json",
    help="Format for the output file or stdout when no output file is specified. Choices: json, junit. Defaults to json.",
    required=False,
    type=click.Choice(["json", "junit"], case_sensitive=False),
)
@click.option(
    "--output-only-failures",
    help="If passed then only failures will be included in the output file.",
    is_flag=True,
)
@click.option(
    "--show-all-failures",
    help="If passed then all failures will be printed to the console.",
    is_flag=True,
)
@click.option("-v", "--verbosity", help="Verbosity.", default=0, count=True)
@click.pass_context
@click.version_option()
def cli(
    ctx: click.Context,
    check: str,
    config_file: PurePath,
    create_pr_comment_file: bool,
    only: str,
    output_file: Path | None,
    output_format: str,
    output_only_failures: bool,
    show_all_failures: bool,
    verbosity: int,
) -> None:
    """Entrypoint for dbt-bouncer."""
    if ctx.invoked_subcommand is None:
        config_file_source = ctx.get_parameter_source("config_file").name  # type: ignore[union-attr]
        exit_code = run_bouncer(
            check=check,
            config_file=config_file,
            create_pr_comment_file=create_pr_comment_file,
            only=only,
            output_file=output_file,
            output_format=output_format,
            output_only_failures=output_only_failures,
            show_all_failures=show_all_failures,
            verbosity=verbosity,
            config_file_source=config_file_source,
        )
        ctx.exit(exit_code)


@cli.command()
def init() -> None:
    """Create a basic dbt-bouncer.yml file.

    Raises:
        RuntimeError: If the config file already exists.

    """
    config_content = """# dbt-bouncer configuration file
# This file is used to configure dbt-bouncer checks.

dbt_artifacts_dir: target # Directory where dbt artifacts (manifest.json, etc.) are located.

manifest_checks:
  - name: check_model_description_populated
    description: All models must have a description.

  - name: check_model_names
    description: Models in the staging layer should always start with "stg_".
    include: ^models/staging
    model_name_pattern: ^stg_

  - name: check_model_has_unique_test
    description: All models must have a unique test defined.

# Example: check that relies on `catalog.json` being present
# catalog_checks:
#   - name: check_column_description_populated
#     description: All columns in the marts layer must have a description.
#     include: ^models/marts
"""
    configure_console_logging(verbosity=0)

    config_path = Path("dbt-bouncer.yml")
    if config_path.exists():
        raise RuntimeError(f"{config_path} already exists.")

    with Path(config_path).open("w") as f:
        f.write(config_content)

    logging.info(f"Created `{config_path}`.")


@cli.command(name="list")
def list_checks() -> None:
    """List all available dbt-bouncer checks, grouped by category."""
    import itertools

    from dbt_bouncer.utils import get_check_objects

    # Map module path segment -> display category name
    category_labels = {
        "catalog": "catalog_checks",
        "manifest": "manifest_checks",
        "run_results": "run_results_checks",
    }

    def category_key(check_class: type) -> str:
        # e.g. "dbt_bouncer.checks.manifest.check_models" -> "manifest"
        parts = check_class.__module__.split(".")
        return parts[2] if len(parts) > 2 else "other"

    checks = sorted(get_check_objects(), key=lambda c: (category_key(c), c.__name__))
    for category, group in itertools.groupby(checks, key=category_key):
        label = category_labels.get(category, category)
        click.echo(f"{label}:")
        for check_class in group:
            docstring = (check_class.__doc__ or "").strip()
            description = docstring.splitlines()[0] if docstring else ""
            click.echo(f"  {check_class.__name__}:\n      {description}\n")
