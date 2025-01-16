# mypy: disable-error-code="arg-type,attr-defined,union-attr"

"""Re-usable functions for dbt-bouncer."""

import importlib
import logging
import os
import re
import sys
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, List, Mapping, Type

import click
import yaml
from packaging.version import Version as PyPIVersion
from semver import Version

if TYPE_CHECKING:
    from dbt_bouncer.check_base import BaseCheck


def clean_path_str(path: str) -> str:
    """Clean a path string by replacing double backslashes with a forward slash.

    Returns:
        str: Cleaned path string.

    """
    return path.replace("\\", "/") if path is not None else ""


def create_github_comment_file(
    failed_checks: List[List[str]], show_all_failures: bool
) -> None:
    """Create a markdown file containing a comment for GitHub."""
    md_formatted_comment = make_markdown_table(
        [
            ["Check name", "Failure message"],
            *sorted(failed_checks if show_all_failures else failed_checks[:25]),
        ],
    )

    # Would like to be more specific and include the job ID, but it's not exposed as an environment variable: https://github.com/actions/runner/issues/324
    md_formatted_comment = f"## **Failed `dbt-bouncer`** checks\n\n{md_formatted_comment}\n\nSent from this [GitHub Action workflow run](https://github.com/{os.environ.get('GITHUB_REPOSITORY', None)}/actions/runs/{os.environ.get('GITHUB_RUN_ID', None)})."
    if len(failed_checks) > 25 and not show_all_failures:
        md_formatted_comment += f"\n\n**Note:** Only the first 25 failed checks (of {len(failed_checks)}) are shown."

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
def get_check_objects() -> List[Type["BaseCheck"]]:
    """Get list of Check* classes and check_* functions.

    Returns:
        List[Type[BaseCheck]]: List of Check* classes.

    """

    def import_check(check_class_name: str, check_file_path: str) -> None:
        """Import the Check* class to locals()."""
        spec = importlib.util.spec_from_file_location(check_class_name, check_file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        sys._getframe().f_locals[check_class_name] = module
        check_objects.append(locals()[check_class_name])

    check_objects: List[Type["BaseCheck"]] = []
    check_files = [
        f for f in (Path(__file__).parent / "checks").glob("*/*.py") if f.is_file()
    ]
    for check_file in check_files:
        check_qual_name = ".".join(
            [x.replace(".py", "") for x in check_file.parts[-4:]]
        )
        imported_check_file = importlib.import_module(check_qual_name)

        for obj in [i for i in dir(imported_check_file) if i.startswith("Check")]:
            import_check(obj, check_file.absolute().__str__())

    config_file_path = click.get_current_context().obj["config_file_path"]
    custom_checks_dir = click.get_current_context().obj["custom_checks_dir"]
    if custom_checks_dir is not None:
        custom_checks_dir = config_file_path.parent / custom_checks_dir
        logging.debug(f"{custom_checks_dir=}")
        custom_check_files = [
            f for f in Path(custom_checks_dir).glob("*/*.py") if f.is_file()
        ]
        logging.debug(f"{custom_check_files=}")

        for check_file in custom_check_files:
            spec = importlib.util.spec_from_file_location("custom_check", check_file)
            foo = importlib.util.module_from_spec(spec)
            sys.modules["custom_check"] = foo
            spec.loader.exec_module(foo)
            for obj in dir(foo):
                if obj.startswith("Check"):
                    import_check(obj, check_file.absolute().__str__())

    logging.debug(f"Loaded {len(check_objects)} `Check*` classes.")

    return check_objects


def get_package_version_number(version_string: str) -> Version:
    """Dbt Cloud no longer uses version numbers that comply with semantic versioning, e.g. "2024.11.06+2a3d725".
    This function is used to convert the version number to a version object that can be used to compare versions.

    Args:
        version_string (str): The version number to convert.

    Returns:
            Version: The version object.

    """  # noqa: D205
    p = PyPIVersion(version_string)

    return Version(*p.release)


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
