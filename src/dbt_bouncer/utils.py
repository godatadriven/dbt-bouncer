"""RE-usable functions for dbt-bouncer."""

import logging
import os
import re
from pathlib import Path
from typing import Any, List, Mapping

import toml
import yaml


def create_github_comment_file(failed_checks: List[List[str]]) -> None:
    """Create a markdown file containing a comment for GitHub."""
    md_formatted_comment = make_markdown_table(
        [["Check name", "Failure message"], *sorted(failed_checks)],
    )

    md_formatted_comment = f"## **Failed `dbt-bouncer`** checks\n\n{md_formatted_comment}\n\nSent from this [GitHub Action workflow run](https://github.com/{os.environ.get('GITHUB_REPOSITORY', None)}/actions/runs/{os.environ.get('GITHUB_RUN_ID', None)})."  # Would like to be more specific and include the job ID, but it's not exposed as an environment variable: https://github.com/actions/runner/issues/324

    logging.debug(f"{md_formatted_comment=}")

    if os.environ.get("GITHUB_REPOSITORY", None) is not None:
        comment_file = "/app/github-comment.md"
    else:
        comment_file = "github-comment.md"

    logging.info(f"Writing comment for GitHub to {comment_file}...")
    with Path.open(Path(comment_file), "w") as f:
        f.write(md_formatted_comment)


def resource_in_path(check, resource) -> bool:
    """Validate that a check is applicable to a specific resource path.

    Returns:
        bool: Whether the check is applicable to the resource.

    """
    return object_in_path(check.include, resource.original_file_path) and not (
        check.exclude is not None
        and object_in_path(check.exclude, resource.original_file_path)
    )


def find_missing_meta_keys(meta_config, required_keys) -> List[str]:
    """Find missing keys in a meta config.

    Returns:
        List[str]: List of missing keys.

    """
    # Get all keys in meta config
    keys_in_meta = list(flatten(meta_config).keys())

    # Get required keys and convert to a list
    required_keys = [
        re.sub(r"(\>{1}\d{1,10})", "", f"{k}>{v}")
        for k, v in flatten(required_keys).items()
    ]

    return [x for x in required_keys if x not in keys_in_meta]


def flatten(structure: Any, key: str = "", path: str = "", flattened=None):
    """Take a dict of arbitrary depth that may contain lists and return a non-nested dict of all pathways.

    Returns:
        dict: Flattened dict.

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


def get_config_file_path(
    config_file: str,
    config_file_source: str,
) -> Path:
    """Get the path to the config file for dbt-bouncer. This is fetched from (in order):

    1. The file passed via the `--config-file` CLI flag.
    2. A file named `dbt-bouncer.yml` in the current working directory.
    3. A `[tool.dbt-bouncer]` section in `pyproject.toml` (in current working directory or parent directories).

    Returns:
        Path: Config file for dbt-bouncer.

    Raises:
        RuntimeError: If no config file is found.

    """  # noqa: D400, D415
    logging.debug(f"{config_file=}")
    logging.debug(f"{config_file_source=}")

    if config_file_source == "COMMANDLINE":
        logging.debug(f"Config file passed via command line: {config_file}")
        return Path(config_file)

    if config_file_source == "DEFAULT":
        logging.debug(f"Using default value for config file: {config_file}")
        config_file_path = Path.cwd() / config_file
        if config_file_path.exists():
            return config_file_path

    # Read config from pyproject.toml
    logging.info("Loading config from pyproject.toml, if exists...")
    if (Path().cwd() / "pyproject.toml").exists():
        pyproject_toml_dir = Path().cwd()
    else:
        pyproject_toml_dir = next(
            (
                parent
                for parent in Path().cwd().parents
                if (parent / "pyproject.toml").exists()
            ),
            None,  # type: ignore[arg-type]
        )  # i.e. look in parent directories for a pyproject.toml file

        if pyproject_toml_dir is None:
            logging.debug("No pyproject.toml found.")
            raise RuntimeError(
                "No pyproject.toml found. Please ensure you have a pyproject.toml file in the root of your project correctly configured to work with `dbt-bouncer`. Alternatively, you can pass the path to your config file via the `--config-file` flag.",
            )

    return pyproject_toml_dir / "pyproject.toml"


def load_config_file_contents(config_file_path: Path) -> Mapping[str, Any]:
    """Load the contents of the config file.

    Returns:
        Mapping[str, Any]: Config for dbt-bouncer.

    Raises:
        RuntimeError: If the config file type is not supported or does not contain the expected keys.

    """
    if config_file_path.suffix in [".yml", ".yaml"]:
        return load_config_from_yaml(config_file_path)
    elif config_file_path.suffix in [".toml"]:
        toml_cfg = toml.load(config_file_path)
        if "dbt-bouncer" in toml_cfg["tool"]:
            return next(v for k, v in toml_cfg["tool"].items() if k == "dbt-bouncer")
        else:
            raise RuntimeError(
                "Please ensure your pyproject.toml file is correctly configured to work with `dbt-bouncer`. Alternatively, you can pass the path to your config file via the `--config-file` flag.",
            )
    else:
        raise RuntimeError(
            f"Config file must be either a `pyproject.toml`, `.yaml` or `.yaml file. Got {config_file_path.suffix}."
        )


def load_config_from_yaml(config_file: Path) -> Mapping[str, Any]:
    """Safely load a YAML file to a dict object.

    Returns:
        Mapping[str, Any]: Dict object.

    Raises:
        FileNotFoundError: If the config file does not exist.

    """
    config_path = Path().cwd() / config_file
    logging.debug(f"Loading config from {config_path}...")
    logging.debug(f"Loading config from {config_file}...")
    if (
        not config_path.exists()
    ):  # Shouldn't be needed as click should have already checked this
        raise FileNotFoundError(f"No config file found at {config_path}.")

    with Path.open(config_path, "r") as f:
        conf = yaml.safe_load(f)

    logging.info(f"Loaded config from {config_file}...")

    return conf


def make_markdown_table(array: List[List[str]]) -> str:
    """Transform a list of lists into a markdown table. The first element is the header row.

    Returns:
        str: Resulting markdown table.

    """
    nl = "\n"

    markdown = nl
    markdown += f"| {' | '.join(array[0])} |"

    markdown += nl
    markdown += f"| {' | '.join([':---'] * len(array[0]))} |"

    markdown += nl
    for entry in array[1:]:
        markdown += f"| {' | '.join(entry)} |{nl}"

    return markdown


def object_in_path(include_pattern: str, path: str) -> bool:
    """Determine if an object is included in the specified path pattern.

    If no pattern is specified then all objects are included.

    Returns:
        bool: True if the object is included in the path pattern, False otherwise.

    """
    if include_pattern is not None:
        return re.compile(include_pattern.strip()).match(path) is not None
    else:
        return True
