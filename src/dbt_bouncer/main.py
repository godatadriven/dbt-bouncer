import sys
from pathlib import Path
from typing import Dict, List, Union

import click

from dbt_bouncer.conf_validator import validate_conf
from dbt_bouncer.logger import logger
from dbt_bouncer.parsers import (
    load_dbt_artifact,
    parse_catalog_artifact,
    parse_manifest_artifact,
    parse_run_results_artifact,
)
from dbt_bouncer.runner import runner
from dbt_bouncer.utils import get_dbt_bouncer_config
from dbt_bouncer.version import version


@click.command()
@click.option(
    "--config-file",
    default=Path("dbt-bouncer.yml"),
    help="Location of the YML config file.",
    required=False,
    type=Path,
)
@click.option(
    "--create-pr-comment-file",
    default=False,
    help="Create a `github-comment.md` file that will be sent to GitHub as a PR comment. Defaults to True when run as a GitHub Action.",
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
@click.version_option()
def cli(
    config_file: Path,
    create_pr_comment_file: bool,
    output_file: Union[None, Path],
) -> None:
    logger.info(f"Running dbt-bouncer ({version()})...")

    # Validate output file has `.json` extension
    if output_file and not output_file.suffix == ".json":
        raise RuntimeError(
            f"Output file must have a `.json` extension. Got `{output_file.suffix}`."
        )

    conf = get_dbt_bouncer_config(
        config_file=config_file,
        config_file_source=click.get_current_context().get_parameter_source("config_file").name,  # type: ignore[union-attr]
    )
    logger.debug(f"{conf=}")
    bouncer_config = validate_conf(conf=conf)
    logger.debug(f"{bouncer_config=}")

    check_categories = [
        k
        for k in dir(bouncer_config)
        if k.endswith("_checks") and getattr(bouncer_config, k) != []
    ]
    logger.debug(f"{check_categories=}")

    # Add indices to uniquely identify checks
    for category in check_categories:
        for idx, c in enumerate(getattr(bouncer_config, category)):
            c.index = idx

    config: Dict[str, List[Dict[str, str]]] = {}
    for category in check_categories:
        for check_name in set([c.name for c in getattr(bouncer_config, category)]):
            config[check_name] = []
            for check in getattr(bouncer_config, category):
                if check.name == check_name:
                    config[check_name].append(
                        {k: v for k, v in check.model_dump().items() if k != "name"}
                    )
    logger.debug(f"{config=}")

    dbt_artifacts_dir = config_file.parent / bouncer_config.dbt_artifacts_dir

    # Manifest, will always be parsed
    manifest_obj = load_dbt_artifact(
        artifact_name="manifest.json",
        dbt_artifacts_dir=dbt_artifacts_dir,
    )

    project_exposures, project_macros, project_models, project_tests, project_sources = (
        parse_manifest_artifact(
            artifact_dir=dbt_artifacts_dir,
            manifest_obj=manifest_obj,
        )
    )

    # Catalog, must come after manifest is parsed
    if bouncer_config.catalog_checks != []:
        project_catalog_nodes, project_catalog_sources = parse_catalog_artifact(
            artifact_dir=dbt_artifacts_dir,
            manifest_obj=manifest_obj,
        )
    else:
        project_catalog_nodes = []
        project_catalog_sources = []

    # Run results, must come after manifest is parsed
    if bouncer_config.run_results_checks != []:
        project_run_results = parse_run_results_artifact(
            artifact_dir=dbt_artifacts_dir,
            manifest_obj=manifest_obj,
        )
    else:
        project_run_results = []

    logger.info("Running checks...")
    results = runner(
        bouncer_config=config,
        catalog_nodes=project_catalog_nodes,
        catalog_sources=project_catalog_sources,
        create_pr_comment_file=create_pr_comment_file,
        exposures=project_exposures,
        macros=project_macros,
        manifest_obj=manifest_obj,
        models=project_models,
        output_file=output_file,
        run_results=project_run_results,
        sources=project_sources,
        tests=project_tests,
    )
    sys.exit(results[0])
