"""Re-usable functions for dbt-bouncer."""

# TODO Remove after this program no longer support Python 3.8.*
from __future__ import annotations

import importlib
import logging
import os
import re
from functools import lru_cache
from importlib.machinery import SourceFileLoader
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Union

import yaml

if TYPE_CHECKING:
    import function


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


@lru_cache
def get_check_objects() -> Dict[str, Union[function, Any]]:
    """Get list of Check* classes and check_* functions.

    Returns:
        Dict[str, Union[function, Any]]: Dictionary of Check* classes and check_* functions.

    """
    check_objects: Dict[str, Union[function, Any]] = {
        "classes": [],
        "functions": [],
    }
    check_files = [
        f for f in (Path(__file__).parent / "checks").glob("*/*.py") if f.is_file()
    ]
    for check_file in check_files:
        check_qual_name = ".".join(
            [x.replace(".py", "") for x in check_file.parts[-4:]]
        )
        imported_check_file = importlib.import_module(check_qual_name)

        for obj in dir(imported_check_file):
            loader = SourceFileLoader(obj, check_file.absolute().__str__())
            spec = importlib.util.spec_from_loader(loader.name, loader)
            locals()[obj] = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
            loader.exec_module(locals()[obj])
            if obj.startswith("Check"):
                check_objects["classes"].append(locals()[obj])
            elif obj.startswith("check_"):
                check_objects["functions"].append(locals()[obj])

    logging.debug(
        f"Loaded {len(check_objects['classes'])} `Check*` classes and {len(check_objects['functions'])} `check)*` functions."
    )
    return check_objects


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
