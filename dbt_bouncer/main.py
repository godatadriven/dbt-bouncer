import json
from enum import Enum
from pathlib import Path
from typing import Dict, List

import click
from dbt_artifacts_parser.parser import parse_manifest

from dbt_bouncer.conf_validator import validate_conf
from dbt_bouncer.logger import logger
from dbt_bouncer.runner import runner
from dbt_bouncer.utils import get_dbt_bouncer_config
from dbt_bouncer.version import version


@click.command()
@click.option(
    "--config-file",
    default=Path("dbt-bouncer.yml"),
    help="Location of the YML config file.",
    required=False,
    type=click.Path(exists=False),
)
@click.option(
    "--send-pr-comment",
    default=False,
    help="Send a comment to the GitHub PR with a list of failed checks. Defaults to True when run as a GitHub Action.",
    required=False,
    type=click.BOOL,
)
@click.version_option()
def cli(config_file, send_pr_comment: bool):
    logger.info(f"Running dbt-bouncer ({version()})...")

    conf = get_dbt_bouncer_config(
        config_file=config_file,
        config_file_source=click.get_current_context().get_parameter_source("config_file").name,  # type: ignore[union-attr]
    )
    logger.debug(f"{conf=}")
    bouncer_config = validate_conf(conf=conf).model_dump()
    logger.debug(f"{bouncer_config=}")

    # Add indices to uniquely identify checks
    for k in bouncer_config.keys():
        if k.endswith("_checks"):
            for idx, c in enumerate(bouncer_config[k]):
                c["index"] = idx

    config: Dict[str, List[Dict[str, str]]] = {}
    for check_name in set([c["name"] for c in bouncer_config["manifest_checks"]]):
        config[check_name] = []
        for check in bouncer_config["manifest_checks"]:
            if check["name"] == check_name:
                config[check_name].append(
                    {k: check[k] for k in set(list(check.keys())) - set(["name"])}
                )
    logger.debug(f"{config=}")

    # Load manifest
    manifest_json_path = (
        (Path(__file__).parent.parent / config_file).parent
        / bouncer_config.get("dbt_artifacts_dir", "./target")
        / "manifest.json"
    )
    logger.debug(f"Loading manifest.json from {manifest_json_path}...")
    logger.info(
        f"Loading manifest.json from {bouncer_config.get('dbt_artifacts_dir', './target')}/manifest.json..."
    )
    if not manifest_json_path.exists():
        raise FileNotFoundError(
            f"No manifest.json found at {bouncer_config.get('dbt_artifacts_dir', './target')}/manifest.json."
        )

    with Path.open(manifest_json_path, "r") as fp:
        manifest_obj = parse_manifest(manifest=json.load(fp))

    logger.debug(f"{manifest_obj.metadata.project_name=}")

    project_macros = []
    for _, v in manifest_obj.macros.items():
        if v.package_name == manifest_obj.metadata.project_name:
            project_macros.append(v.model_dump())

    project_models = []
    project_tests = []
    for _, v in manifest_obj.nodes.items():
        if v.package_name == manifest_obj.metadata.project_name:
            if (
                isinstance(v.resource_type, Enum) and v.resource_type.value == "model"
            ) or v.resource_type == "model":  # dbt 1.6  # dbt 1.7+
                project_models.append(v.model_dump())
            elif (
                isinstance(v.resource_type, Enum) and v.resource_type.value == "test"
            ) or v.resource_type == "test":  # dbt 1.6  # dbt 1.7+
                project_tests.append(v.model_dump())

    project_sources = []
    for _, v in manifest_obj.sources.items():
        if v.package_name == manifest_obj.metadata.project_name:
            project_sources.append(v.model_dump())

    logger.info(
        f"Parsed `{manifest_obj.metadata.project_name}` project, found {len(project_macros)} macros, {len(project_models)} nodes, {len(project_sources)} sources and {len(project_tests)} tests."
    )

    logger.info("Running checks...")
    runner(
        bouncer_config=config,
        macros=project_macros,
        manifest_obj=manifest_obj,
        models=project_models,
        send_pr_comment=send_pr_comment,
        sources=project_sources,
        tests=project_tests,
    )
