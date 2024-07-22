import json
from pathlib import Path
from typing import Dict, List

import click
from dbt_artifacts_parser.parser import parse_manifest

from dbt_bouncer.config_validator import validate_config_file
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
@click.version_option()
def cli(config_file):
    logger.info(f"Running dbt-bouncer ({version()})...")

    # Load config
    config_path = Path(config_file)
    logger.info(f"Loading config from {config_path}...")
    if not config_path.exists():  # Shouldn't be needed as click should have already checked this
        raise FileNotFoundError(f"No config file found at {config_path}.")

    bouncer_config = validate_config_file(file=config_path).model_dump()

    # Add indices to uniquely identify checks
    for idx, c in enumerate(bouncer_config["checks"]):
        c["index"] = idx

    config: Dict[str, List[Dict[str, str]]] = {}
    for check_name in set([c["name"] for c in bouncer_config["checks"]]):
        config[check_name] = []
        for check in bouncer_config["checks"]:
            if check["name"] == check_name:
                config[check_name].append(
                    {k: check[k] for k in set(list(check.keys())) - set(["name"])}
                )
    logger.debug(f"{config=}")

    # Load manifest
    manifest_json_path = (
        Path(bouncer_config.get("dbt_artifacts_dir", "./target")) / "manifest.json"
    )
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
        bouncer_config=config,
        manifest_obj=manifest_obj,
        models=project_models,
        sources=project_sources,
        tests=project_tests,
    )
