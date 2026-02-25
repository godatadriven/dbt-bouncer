import logging
from pathlib import Path, PurePath
from typing import Annotated, Optional

import typer
from typer.main import get_command

from dbt_bouncer.logger import configure_console_logging
from dbt_bouncer.version import version as get_version

app = typer.Typer(
    no_args_is_help=False,
    context_settings={"help_option_names": ["-h", "--help"]},
)


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
        output_format: Format for the output file or stdout (csv, json, junit, sarif, tap).
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
    from dbt_bouncer.context import BouncerContext, _rebuild_bouncer_context
    from dbt_bouncer.runner import runner

    _rebuild_bouncer_context()

    ctx = BouncerContext(
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
    results = runner(ctx=ctx)
    return results[0]


@app.callback(invoke_without_command=True)
def main_callback(
    ctx: typer.Context,
    config_file: Annotated[
        Path,
        typer.Option(help="Location of the YML config file."),
    ] = Path("dbt-bouncer.yml"),
    create_pr_comment_file: Annotated[
        bool,
        typer.Option(
            hidden=True,
            help="Create a `github-comment.md` file that will be sent to GitHub as a PR comment. Defaults to True when `dbt-bouncer` is run as a GitHub Action.",
        ),
    ] = False,
    check: Annotated[
        str,
        typer.Option(
            help="Limit the checks run to specific check names, comma-separated. Examples: 'check_model_has_unique_test', 'check_model_names,check_source_freshness_populated'."
        ),
    ] = "",
    only: Annotated[
        str,
        typer.Option(
            help="Limit the checks run to specific categories, comma-separated. Examples: 'manifest_checks', 'catalog_checks,manifest_checks'."
        ),
    ] = "",
    output_file: Annotated[
        Optional[Path],
        typer.Option(help="Location of the file where check metadata will be saved."),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            help="Format for the output file or stdout when no output file is specified. Choices: csv, json, junit, sarif, tap. Defaults to json.",
            case_sensitive=False,
        ),
    ] = "json",
    output_only_failures: Annotated[
        bool,
        typer.Option(
            help="If passed then only failures will be included in the output file."
        ),
    ] = False,
    show_all_failures: Annotated[
        bool,
        typer.Option(
            help="If passed then all failures will be printed to the console."
        ),
    ] = False,
    verbosity: Annotated[
        int,
        typer.Option("-v", "--verbosity", help="Verbosity.", count=True),
    ] = 0,
    version: Annotated[
        bool,
        typer.Option("--version", help="Show version and exit."),
    ] = False,
) -> None:
    """Entrypoint for dbt-bouncer.

    When invoked without a subcommand, runs checks for backwards compatibility.
    Use 'dbt-bouncer run' for the explicit command.

    Raises:
        Exit: If the version flag is passed or an invalid output format is provided.

    """
    # Handle version flag
    if version:
        typer.echo(get_version())
        raise typer.Exit()

    # Validate output format
    valid_formats = ["csv", "json", "junit", "sarif", "tap"]
    if output_format.lower() not in valid_formats:
        typer.echo(
            f"Error: Invalid output format '{output_format}'. Choose from: {', '.join(valid_formats)}"
        )
        raise typer.Exit(1)

    if ctx.invoked_subcommand is None:
        # Determine config file source
        config_file_source = (
            "COMMANDLINE" if config_file != Path("dbt-bouncer.yml") else "DEFAULT"
        )

        exit_code = run_bouncer(
            check=check,
            config_file=config_file,
            create_pr_comment_file=create_pr_comment_file,
            only=only,
            output_file=output_file,
            output_format=output_format.lower(),
            output_only_failures=output_only_failures,
            show_all_failures=show_all_failures,
            verbosity=verbosity,
            config_file_source=config_file_source,
        )
        raise typer.Exit(exit_code)


@app.command()
def run(
    config_file: Annotated[
        Optional[Path],
        typer.Option(help="Location of the YML config file."),
    ] = Path("dbt-bouncer.yml"),
    create_pr_comment_file: Annotated[
        bool,
        typer.Option(
            hidden=True,
            help="Create a `github-comment.md` file that will be sent to GitHub as a PR comment. Defaults to True when `dbt-bouncer` is run as a GitHub Action.",
        ),
    ] = False,
    check: Annotated[
        str,
        typer.Option(
            help="Limit the checks run to specific check names, comma-separated.",
            rich_help_panel="Check Selection",
        ),
    ] = "",
    only: Annotated[
        str,
        typer.Option(
            help="Limit the checks run to specific categories, comma-separated.",
            rich_help_panel="Check Selection",
        ),
    ] = "",
    output_file: Annotated[
        Optional[Path],
        typer.Option(
            help="Location of the file where check metadata will be saved.",
            rich_help_panel="Output Options",
        ),
    ] = None,
    output_format: Annotated[
        str,
        typer.Option(
            help="Format for the output file or stdout when no output file is specified. Choices: csv, json, junit, sarif, tap. Defaults to json.",
            case_sensitive=False,
            rich_help_panel="Output Options",
        ),
    ] = "json",
    output_only_failures: Annotated[
        bool,
        typer.Option(
            help="If passed then only failures will be included in the output file.",
            rich_help_panel="Output Options",
        ),
    ] = False,
    show_all_failures: Annotated[
        bool,
        typer.Option(
            help="If passed then all failures will be printed to the console.",
            rich_help_panel="Display Options",
        ),
    ] = False,
    verbosity: Annotated[
        int,
        typer.Option(
            "-v",
            "--verbosity",
            help="Verbosity.",
            count=True,
            rich_help_panel="Display Options",
        ),
    ] = 0,
) -> None:
    """Run dbt-bouncer checks against your dbt project.

    [bold]Examples:[/bold]

      Run all checks with default config:
        [cyan]$ dbt-bouncer run[/cyan]

      Run specific checks only:
        [cyan]$ dbt-bouncer run --check check_model_names,check_model_has_unique_test[/cyan]

      Run manifest checks only with custom config:
        [cyan]$ dbt-bouncer run --only manifest_checks --config-file my-config.yml[/cyan]

      Save results to JSON file:
        [cyan]$ dbt-bouncer run --output-file results.json --output-format json[/cyan]

    Raises:
        Exit: If an invalid output format is provided or the checks fail.

    """
    # Validate output format
    valid_formats = ["csv", "json", "junit", "sarif", "tap"]
    if output_format.lower() not in valid_formats:
        typer.echo(
            f"Error: Invalid output format '{output_format}'. Choose from: {', '.join(valid_formats)}"
        )
        raise typer.Exit(1)

    # Determine config file source
    config_file_source = (
        "COMMANDLINE" if config_file != Path("dbt-bouncer.yml") else "DEFAULT"
    )

    exit_code = run_bouncer(
        check=check,
        config_file=config_file,
        create_pr_comment_file=create_pr_comment_file,
        only=only,
        output_file=output_file,
        output_format=output_format.lower(),
        output_only_failures=output_only_failures,
        show_all_failures=show_all_failures,
        verbosity=verbosity,
        config_file_source=config_file_source,
    )
    raise typer.Exit(exit_code)


@app.command()
def init() -> None:
    """Create a dbt-bouncer.yml file interactively.

    Asks questions to customize your initial configuration.

    Raises:
        Abort: If the user declines to overwrite an existing config file.

    """
    import yaml
    from rich.console import Console

    console = Console()
    console.print("\n[bold blue]>> dbt-bouncer initialization[/bold blue]\n")

    # Interactive prompts
    artifacts_dir = typer.prompt(
        "Where are your dbt artifacts located?", default="target"
    )

    check_descriptions = typer.confirm("Check for model descriptions?", default=True)

    check_unique_tests = typer.confirm(
        "Check for unique tests on models?", default=True
    )

    check_naming = typer.confirm(
        "Check naming conventions for staging models?", default=True
    )

    # Build config based on answers
    manifest_checks = []

    if check_descriptions:
        manifest_checks.append(
            {
                "name": "check_model_description_populated",
                "description": "All models must have a description.",
            }
        )

    if check_unique_tests:
        manifest_checks.append(
            {
                "name": "check_model_has_unique_test",
                "description": "All models must have a unique test defined.",
            }
        )

    if check_naming:
        manifest_checks.append(
            {
                "name": "check_model_names",
                "description": "Models in the staging layer should always start with 'stg_'.",
                "include": "^models/staging",
                "model_name_pattern": "^stg_",
            }
        )

    config_dict = {
        "dbt_artifacts_dir": artifacts_dir,
        "manifest_checks": manifest_checks,
    }

    config_path = Path("dbt-bouncer.yml")
    if config_path.exists():
        overwrite = typer.confirm(
            f"\n[yellow]Warning:[/yellow] {config_path} already exists. Overwrite?",
            default=False,
        )
        if not overwrite:
            console.print("[red]Aborted.[/red]")
            raise typer.Abort()

    # Write YAML config
    with Path(config_path).open("w") as f:
        yaml.dump(config_dict, f, default_flow_style=False, sort_keys=False)

    console.print(f"\n[bold green][OK] Created {config_path}[/bold green]")
    console.print(
        f"  Added [cyan]{len(manifest_checks)}[/cyan] checks to get you started.\n"
    )


@app.command()
def validate(
    config_file: Annotated[
        Optional[Path],
        typer.Option(help="Location of the YML config file."),
    ] = Path("dbt-bouncer.yml"),
) -> None:
    """Validate the dbt-bouncer configuration file.

    Checks for YAML syntax errors and common configuration issues,
    reporting line numbers for any issues found.

    Raises:
        Exit: If the config file is valid or if issues are found.
        RuntimeError: If the config file is not found.

    """
    configure_console_logging(verbosity=0)

    config_path = Path("dbt-bouncer.yml") if config_file is None else Path(config_file)

    if not config_path.exists():
        raise RuntimeError(f"Config file not found: {config_path}")

    from dbt_bouncer.config_file_validator import lint_config_file

    issues = lint_config_file(config_path)

    if not issues:
        logging.info("Config file is valid!")
        raise typer.Exit(0)
    else:
        logging.error(f"Found {len(issues)} issue(s) in config file:")
        for issue in issues:
            logging.error(f"  Line {issue['line']}: {issue['message']}")
        raise typer.Exit(1)


@app.command(name="list")
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
        typer.echo(f"{label}:")
        for check_class in group:
            docstring = (check_class.__doc__ or "").strip()
            description = docstring.splitlines()[0] if docstring else ""
            typer.echo(f"  {check_class.__name__}:\n      {description}\n")


# For mkdocs-click compatibility - export the underlying Click command
cli = get_command(app)
