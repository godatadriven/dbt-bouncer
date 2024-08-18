import contextlib
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Union

import toml
import yaml
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Exposures, Macros

from dbt_bouncer.logger import logger
from dbt_bouncer.parsers import (
    DbtBouncerCatalogNode,
    DbtBouncerModel,
    DbtBouncerResult,
    DbtBouncerSource,
)


def create_github_comment_file(failed_checks: List[List[str]]) -> None:
    """
    Create a markdown file containing a comment for GitHub.
    """

    md_formatted_comment = make_markdown_table(
        [["Check name", "Failure message"]] + list(sorted(failed_checks))
    )

    md_formatted_comment = f"## **Failed `dbt-bouncer`** checks\n\n{md_formatted_comment}\n\nSent from this [GitHub Action workflow run](https://github.com/{os.environ.get('GITHUB_REPOSITORY',None)}/actions/runs/{os.environ.get('GITHUB_RUN_ID', None)})."  # Would like to be more specific and include the job ID, but it's not exposed as an environment variable: https://github.com/actions/runner/issues/324

    logger.debug(f"{md_formatted_comment=}")

    if os.environ.get("GITHUB_REPOSITORY", None) is not None:
        comment_file = "/app/github-comment.md"
    else:
        comment_file = "github-comment.md"

    logger.info(f"Writing comment for GitHub to {comment_file}...")
    with open(comment_file, "w") as f:
        f.write(md_formatted_comment)


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


def flatten(structure: Any, key: str = "", path: str = "", flattened=None):
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
    catalog_node: DbtBouncerCatalogNode = None,
    catalog_source: DbtBouncerCatalogNode = None,
    check_config: Union[Dict[str, Union[Dict[str, str], str]], None] = None,
    exposure: Exposures = None,
    macro: Macros = None,
    model: DbtBouncerModel = None,
    request=None,
    run_result: DbtBouncerResult = None,
    source: DbtBouncerSource = None,
) -> Dict[
    str,
    Union[
        DbtBouncerCatalogNode,
        Dict[str, Union[Dict[str, str], str]],
        Exposures,
        Macros,
        DbtBouncerModel,
        DbtBouncerResult,
        DbtBouncerSource,
    ],
]:
    """
    Helper function that is used to account for the difference in how arguments are passed to check functions
    when they are run by `dbt-bouncer` and when they are called by pytest.
    """

    if request is not None:
        catalog_node = getattr(request.node.catalog_node, "node", lambda: None)
        catalog_source = getattr(request.node.catalog_source, "node", lambda: None)
        check_config = request.node.check_config
        exposure = request.node.exposure
        macro = request.node.macro
        model = getattr(request.node.model, "model", lambda: None)
        run_result = getattr(request.node.run_result, "result", lambda: None)
        source = getattr(request.node.source, "source", lambda: None)
    else:
        catalog_node = getattr(catalog_node, "node", lambda: None)
        catalog_source = getattr(catalog_source, "node", lambda: None)
        check_config = check_config
        exposure = exposure
        macro = macro
        model = model
        run_result = getattr(run_result, "result", lambda: None)
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


def get_dbt_bouncer_config(config_file: str, config_file_source: str) -> Mapping[str, Any]:
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
        return load_config_from_yaml(Path(config_file))

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


def load_config_from_yaml(config_file: Path) -> Mapping[str, Any]:

    config_path = Path().cwd() / config_file
    logger.debug(f"Loading config from {config_path}...")
    logger.debug(f"Loading config from {config_file}...")
    if not config_path.exists():  # Shouldn't be needed as click should have already checked this
        raise FileNotFoundError(f"No config file found at {config_path}.")

    with Path.open(config_path, "r") as f:
        conf = yaml.safe_load(f)

    logger.info(f"Loaded config from {config_file}...")

    return conf


def make_markdown_table(array: List[List[str]]) -> str:
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
