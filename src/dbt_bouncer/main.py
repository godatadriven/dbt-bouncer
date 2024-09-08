import logging
from pathlib import Path, PurePath
from typing import Union

import click

from dbt_bouncer.logger import configure_console_logging
from dbt_bouncer.version import version


@click.command()
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
    "--output-file",
    default=None,
    help="Location of the json file where check metadata will be saved.",
    required=False,
    type=Path,
)
@click.option("-v", "--verbosity", help="Verbosity.", default=0, count=True)
@click.pass_context
@click.version_option()
def cli(
    ctx: click.Context,
    config_file: PurePath,
    create_pr_comment_file: bool,
    output_file: Union[None, Path],
    verbosity: int,
) -> None:
    """Entrypoint for dbt-bouncer.

    Raises:
        RuntimeError: If output file has an invalid extension.

    """
    configure_console_logging(verbosity)
    logging.info(f"Running dbt-bouncer ({version()})...")

    # Validate output file has `.json` extension
    if output_file and not output_file.suffix == ".json":
        raise RuntimeError(
            f"Output file must have a `.json` extension. Got `{output_file.suffix}`.",
        )

    # Using local imports to speed up CLI startup
    from dbt_bouncer.config_file_validator import (
        get_config_file_path,
        load_config_file_contents,
    )

    config_file_path = get_config_file_path(
        config_file=config_file,
        config_file_source=click.get_current_context()
        .get_parameter_source("config_file")
        .name,  # type: ignore[union-attr]
    )
    config_file_contents = load_config_file_contents(config_file_path)

    # Handle `severity` at the global level
    if config_file_contents.get("severity"):
        logging.info(
            f"Setting `severity` for all checks to `{config_file_contents['severity']}`."
        )
        for c in config_file_contents["manifest_checks"]:
            c["severity"] = config_file_contents["severity"]

    logging.debug(f"{config_file_contents=}")

    # Set click context object for dbt_bouncer.utils.get_check_objects()
    ctx.obj = {
        "config_file_path": config_file_path,
        "custom_checks_dir": config_file_contents.get("custom_checks_dir"),
    }

    from dbt_bouncer.config_file_validator import validate_conf

    bouncer_config = validate_conf(config_file_contents=config_file_contents)
    del config_file_contents
    logging.debug(f"{bouncer_config=}")

    check_categories = [
        i
        for i in bouncer_config.model_dump()
        if i.endswith("_checks") and getattr(bouncer_config, i) != []
    ]
    logging.debug(f"{check_categories=}")

    for category in check_categories:
        for idx, check in enumerate(getattr(bouncer_config, category)):
            # Add indices to uniquely identify checks
            check.index = idx

            # Handle global `exclude` and `include` args
            if bouncer_config.include and not check.include:
                check.include = bouncer_config.include
            if bouncer_config.exclude and not check.exclude:
                check.exclude = bouncer_config.exclude
    logging.debug(f"{bouncer_config=}")

    dbt_artifacts_dir = config_file.parent / bouncer_config.dbt_artifacts_dir

    from dbt_bouncer.parsers import parse_dbt_artifacts

    (
        manifest_obj,
        project_exposures,
        project_macros,
        project_models,
        project_semantic_models,
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
        run_results=project_run_results,
        semantic_models=project_semantic_models,
        sources=project_sources,
        tests=project_tests,
        unit_tests=project_unit_tests,
    )
    ctx.exit(results[0])
