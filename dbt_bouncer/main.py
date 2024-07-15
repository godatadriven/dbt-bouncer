import json
from pathlib import Path

import click
from dbt_artifacts_parser.parser import parse_manifest

from dbt_bouncer.logger import logger
from dbt_bouncer.version import version


@click.command()
@click.option(
    "--dbt-project-dir",
    help="Directory where the dbt project exists.",
    required=True,
    type=click.Path(exists=True),
)
@click.version_option()
def cli(dbt_project_dir):
    logger.info(f"Running dbt_bouncer ({version()})...")

    # Load manifest
    manifest_json_path = Path(dbt_project_dir) / "target/manifest.json"
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
                project_models.append(v)
            elif v.resource_type == "test":
                project_tests.append(v)

    project_sources = []
    for _, v in manifest_obj.sources.items():
        if v.package_name == manifest_obj.metadata.project_name:
            project_sources.append(v)

    logger.info(
        f"Parsed `{manifest_obj.metadata.project_name}` project, found {len(project_models)} nodes, {len(project_sources)} sources and {len(project_tests)} tests."
    )
