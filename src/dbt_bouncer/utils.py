import contextlib
import json
import re
from pathlib import Path
from typing import List, Literal

import toml
import yaml
from dbt_artifacts_parser.parser import parse_catalog, parse_manifest, parse_run_results

from dbt_bouncer.logger import logger


def find_missing_meta_keys(meta_config, required_keys) -> List[str]:
    """
    Find missing keys in a meta config.
    """

    keys_in_meta = list(flatten(meta_config).keys())

    # Get required keys and convert to a list
    required_keys = [
        re.sub(r"(\>{1}\d{1,10})", "", f"{k}>{v}") for k, v in flatten(required_keys).items()
    ]

    return [x for x in required_keys if x not in keys_in_meta]


def flatten(structure, key="", path="", flattened=None):
    """
    Take a dict of arbitrary depth that may contain lists and return a non-nested dict of all pathways.
    """

    if flattened is None:
        flattened = {}
    if type(structure) not in (dict, list):
        flattened[((path + ">") if path else "") + key] = structure
    elif isinstance(structure, list):
        for i, item in enumerate(structure):
            flatten(item, "%d" % i, path + ">" + key, flattened)
    else:
        for new_key, value in structure.items():
            flatten(value, new_key, path + ">" + key, flattened)
    return flattened


def get_check_inputs(
    catalog_node=None,
    catalog_source=None,
    check_config=None,
    exposure=None,
    macro=None,
    model=None,
    request=None,
    run_result=None,
    source=None,
):
    """
    Helper function that is used to account for the difference in how arguments are passed to check functions
    when they are run by `dbt-bouncer` and when they are called by pytest.
    """

    if request is not None:
        catalog_node = request.node.catalog_node
        catalog_source = request.node.catalog_source
        check_config = request.node.check_config
        exposure = request.node.exposure
        macro = request.node.macro
        model = request.node.model
        run_result = request.node.run_result
        source = request.node.source
    else:
        catalog_node = catalog_node
        catalog_source = catalog_source
        check_config = check_config
        exposure = exposure
        macro = macro
        model = model
        run_result = run_result
        source = source

    return {
        "catalog_node": catalog_node,
        "catalog_source": catalog_source,
        "check_config": check_config,
        "exposure": exposure,
        "macro": macro,
        "model": model,
        "run_result": run_result,
        "source": source,
    }


def get_dbt_bouncer_config(config_file: str, config_file_source: str):
    """
    Get the config for dbt-bouncer. This is fetched from (in order):
        1. The file passed via the `--config=file` CLI flag.
        2. A file named `dbt_bouncer.yml` in the current working directory.
        3. A `[tool.dbt-bouncer]` section in `pyproject.toml` (in current working directory or parent directories).
    """

    logger.debug(f"{config_file=}")
    logger.debug(f"{config_file_source=}")

    if config_file_source == "COMMANDLINE":
        logger.debug(f"Config file passed via command line: {config_file}")
        return load_config_from_yaml(config_file)

    if config_file_source == "DEFAULT":
        logger.debug(f"Using default value for config file: {config_file}")
        with contextlib.suppress(FileNotFoundError):
            return load_config_from_yaml(Path.cwd() / config_file)

    # Read config from pyproject.toml
    logger.info("Loading config from pyproject.toml, if exists...")
    if (Path().cwd() / "pyproject.toml").exists():
        pyproject_toml_dir = Path().cwd()
    else:
        pyproject_toml_dir = next(
            (parent for parent in Path().cwd().parents if (parent / "pyproject.toml").exists()),
            None,  # type: ignore[arg-type]
        )  # i.e. look in parent directories for a pyproject.toml file

    if pyproject_toml_dir is None:
        logger.debug("No pyproject.toml found.")
        raise RuntimeError(
            "No pyproject.toml found. Please ensure you have a pyproject.toml file in the root of your project correctly configured to work with `dbt-bouncer`. Alternatively, you can pass the path to your config file via the `--config-file` flag."
        )
    else:
        logger.debug(f"{pyproject_toml_dir / 'pyproject.toml'=}")

        toml_cfg = toml.load(pyproject_toml_dir / "pyproject.toml")
        if "dbt-bouncer" in toml_cfg["tool"].keys():
            conf = [v for k, v in toml_cfg["tool"].items() if k == "dbt-bouncer"][0]
        else:
            raise RuntimeError(
                "Please ensure your pyproject.toml file is correctly configured to work with `dbt-bouncer`. Alternatively, you can pass the path to your config file via the `--config-file` flag."
            )
    return conf


def load_config_from_yaml(config_file):

    config_path = Path().cwd() / config_file
    logger.debug(f"Loading config from {config_path}...")
    logger.debug(f"Loading config from {config_file}...")
    if not config_path.exists():  # Shouldn't be needed as click should have already checked this
        raise FileNotFoundError(f"No config file found at {config_path}.")

    with Path.open(config_path, "r") as f:
        conf = yaml.safe_load(f)

    logger.info(f"Loaded config from {config_file}...")

    return conf


def load_dbt_artifact(
    artifact_name: Literal["catalog.json", "manifest.json", "run_results.json"],
    dbt_artifacts_dir: str,
):
    """
    Load a dbt artifact from a JSON file to a Pydantic object
    """

    logger.debug(f"{artifact_name=}")
    logger.debug(f"{dbt_artifacts_dir=}")

    artifact_path = dbt_artifacts_dir / Path(artifact_name)
    logger.info(f"Loading {artifact_name} from {artifact_path.absolute()}...")
    if not artifact_path.exists():
        raise FileNotFoundError(f"No {artifact_name} found at {artifact_path.absolute()}.")

    if artifact_name == "catalog.json":
        with Path.open(artifact_path, "r") as fp:
            catalog_obj = parse_catalog(catalog=json.load(fp))

        return catalog_obj

    elif artifact_name == "manifest.json":
        with Path.open(artifact_path, "r") as fp:
            manifest_obj = parse_manifest(manifest=json.load(fp))

        return manifest_obj

    elif artifact_name == "run_results.json":
        with Path.open(artifact_path, "r") as fp:
            run_results_obj = parse_run_results(run_results=json.load(fp))

        return run_results_obj


def make_markdown_table(array):
    """
    Transforms a list of lists into a markdown table. The first element is the header row.
    """

    nl = "\n"

    markdown = nl
    markdown += f"| {' | '.join(array[0])} |"

    markdown += nl
    markdown += f"| {' | '.join([':---']*len(array[0]))} |"

    markdown += nl
    for entry in array[1:]:
        markdown += f"| {' | '.join(entry)} |{nl}"

    return markdown


def object_in_path(include_pattern: str, path: str) -> bool:
    """
    Determine if an object is included in the specified path pattern.
    If no pattern is specified then all objects are included
    """

    if include_pattern is not None:
        return re.compile(include_pattern.strip()).match(path) is not None
    else:
        return True
