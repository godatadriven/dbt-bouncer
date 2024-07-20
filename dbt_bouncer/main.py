import json
from pathlib import Path

import click
import yaml
from dbt_artifacts_parser.parser import parse_manifest

from dbt_bouncer.logger import logger
from dbt_bouncer.runner import runner
from dbt_bouncer.version import version


@click.command()
@click.option(
    "--config-file",
    default=Path("dbt-bouncer.yml"),
    help="Location of the YML config file.",
    required=False,
    type=click.Path(exists=True),
)
@click.option(
    "--dbt-artifacts-dir",
    help="Directory where the dbt artifacts exists, generally the `target` directory inside a dbt project.",
    required=True,
    type=click.Path(exists=True),
)
@click.version_option()
def cli(config_file, dbt_artifacts_dir):
    logger.info(f"Running dbt-bouncer ({version()})...")

    # Load config
    config_path = Path(config_file)
    logger.info(f"Loading config from {config_path}...")
    if not config_path.exists():  # Shouldn't be needed as click should have already checked this
        raise FileNotFoundError(f"No config file found at {config_path}.")

    with Path.open(config_path, "r") as fp:
        bouncer_config = yaml.safe_load(fp)

    # Load manifest
    manifest_json_path = Path(dbt_artifacts_dir) / "manifest.json"
    logger.info(f"Loading manifest.json from {manifest_json_path}...")
    if not manifest_json_path.exists():
        raise FileNotFoundError(f"No manifest.json found at {manifest_json_path}.")

    with Path.open(manifest_json_path, "r") as fp:
        manifest_obj = parse_manifest(manifest=json.load(fp))

    logger.debug(f"{manifest_obj.metadata.project_name=}")

    project_models = []
    project_tests = []
    for _, v in manifest_obj.nodes.items():
        if v.package_name == manifest_obj.metadata.project_name:
            if v.resource_type == "model":
                project_models.append(v.model_dump())
            elif v.resource_type == "test":
                project_tests.append(v.model_dump())

    project_sources = []
    for _, v in manifest_obj.sources.items():
        if v.package_name == manifest_obj.metadata.project_name:
            project_sources.append(v.model_dump())

    logger.info(
        f"Parsed `{manifest_obj.metadata.project_name}` project, found {len(project_models)} nodes, {len(project_sources)} sources and {len(project_tests)} tests."
    )

    logger.info("Running checks...")
    runner(
        bouncer_config=bouncer_config,
        models=project_models,
        sources=project_sources,
        tests=project_tests,
    )
