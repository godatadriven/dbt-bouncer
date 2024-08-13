import json
import sys
from enum import Enum
from pathlib import Path
from typing import Dict, List, Union

import click

from dbt_bouncer.conf_validator import validate_conf
from dbt_bouncer.logger import logger
from dbt_bouncer.runner import runner
from dbt_bouncer.utils import get_dbt_bouncer_config, load_dbt_artifact
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
    "--output-file",
    default=None,
    help="Location of the json file where check metadata will be saved.",
    required=False,
    type=Path,
)
@click.option(
    "--send-pr-comment",
    default=False,
    help="Send a comment to the GitHub PR with a list of failed checks. Defaults to True when run as a GitHub Action.",
    required=False,
    type=click.BOOL,
)
@click.version_option()
def cli(config_file: Path, output_file: Union[None, Path], send_pr_comment: bool):
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
    bouncer_config = validate_conf(conf=conf).model_dump()
    logger.debug(f"{bouncer_config=}")

    check_categories = [k for k in bouncer_config.keys() if k.endswith("_checks")]
    logger.debug(f"{check_categories=}")

    # Add indices to uniquely identify checks
    for category in check_categories:
        for idx, c in enumerate(bouncer_config[category]):
            c["index"] = idx

    config: Dict[str, List[Dict[str, str]]] = {}
    for category in check_categories:
        for check_name in set([c["name"] for c in bouncer_config[category]]):
            config[check_name] = []
            for check in bouncer_config[category]:
                if check["name"] == check_name:
                    config[check_name].append(
                        {k: check[k] for k in set(list(check.keys())) - set(["name"])}
                    )
    logger.debug(f"{config=}")

    # Manifest, will always be parsed
    manifest_obj = load_dbt_artifact(
        artifact_name="manifest.json",
        dbt_artifacts_dir=config_file.parent / bouncer_config.get("dbt_artifacts_dir", "./target"),
    )

    project_exposures = []
    for _, v in manifest_obj.exposures.items():
        if v.package_name == manifest_obj.metadata.project_name:
            project_exposures.append(v.model_dump())

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
                project_models.append(json.loads(v.model_dump_json()))
            elif (
                isinstance(v.resource_type, Enum) and v.resource_type.value == "test"
            ) or v.resource_type == "test":  # dbt 1.6  # dbt 1.7+
                project_tests.append(v.model_dump())

    project_sources = []
    for _, v in manifest_obj.sources.items():
        if v.package_name == manifest_obj.metadata.project_name:
            project_sources.append(v.model_dump())

    logger.info(
        f"Parsed `manifest.json`, found `{manifest_obj.metadata.project_name}` project, found {len(project_exposures)} exposures, {len(project_macros)} macros, {len(project_models)} nodes, {len(project_sources)} sources and {len(project_tests)} tests."
    )

    # Catalog, must come after manifest is parsed
    if bouncer_config["catalog_checks"] != []:
        catalog_obj = load_dbt_artifact(
            artifact_name="catalog.json",
            dbt_artifacts_dir=config_file.parent
            / bouncer_config.get("dbt_artifacts_dir", "./target"),
        )

        project_catalog_nodes = []
        for k, v in catalog_obj.nodes.items():
            if k.split(".")[-2] == manifest_obj.metadata.project_name:
                catalog_node = v.model_dump()
                catalog_node["path"] = manifest_obj.nodes[k].path
                project_catalog_nodes.append(catalog_node)

        project_catalog_sources = []
        for (
            k,
            v,
        ) in (
            catalog_obj.sources.items()
        ):  # Doesn't work with dbt-duckdb for some reason, need to test using different adapter
            if k.split(".")[1] == manifest_obj.metadata.project_name:
                catalog_source = v.model_dump()
                catalog_source["path"] = manifest_obj.sources[k].path
                project_catalog_sources.append(catalog_source)

        logger.info(
            f"Parsed `catalog.json`, found {len(project_catalog_nodes)} nodes and {len(project_catalog_sources)} sources."
        )
    else:
        project_catalog_nodes = []
        project_catalog_sources = []

    # Run results, must come after manifest is parsed
    if bouncer_config["run_results_checks"] != []:
        run_results_obj = load_dbt_artifact(
            artifact_name="run_results.json",
            dbt_artifacts_dir=config_file.parent
            / bouncer_config.get("dbt_artifacts_dir", "./target"),
        )

        project_run_results = []
        for r in run_results_obj.results:
            if r.unique_id.split(".")[-3] == manifest_obj.metadata.project_name:
                run_result = r.model_dump()
                run_result["path"] = manifest_obj.nodes[r.unique_id].path
                project_run_results.append(run_result)

        logger.info(f"Parsed `run_results.json`, found {len(project_run_results)} results.")
    else:
        project_run_results = []

    logger.info("Running checks...")
    results = runner(
        bouncer_config=config,
        catalog_nodes=project_catalog_nodes,
        catalog_sources=project_catalog_sources,
        exposures=project_exposures,
        macros=project_macros,
        manifest_obj=manifest_obj,
        models=project_models,
        output_file=output_file,
        run_results=project_run_results,
        send_pr_comment=send_pr_comment,
        sources=project_sources,
        tests=project_tests,
    )
    sys.exit(results[0])
