"""Re-usable functions for dbt-bouncer."""

import importlib
import importlib.util
import inspect
import logging
import os
import re
import sys
from collections.abc import Mapping
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Any

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
    failed_checks: list[list[str]], show_all_failures: bool
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


def get_nested_value(
    d: dict[str, Any], keys: list[str], default: Any | None = None
) -> Any:
    """Retrieve a value from a nested dictionary using a list of keys.

    This function safely traverses a dictionary structure, allowing access to
    deeply nested values. If any key in the `keys` list does not exist at
    its respective level, the function returns the specified default value.

    Args:
        d: The dictionary to traverse.
        keys: A list of strings representing the sequence of keys to follow
              to reach the desired nested value.
        default: The value to return if any key in the `keys` list is not
                 found at its corresponding level, or if an intermediate
                 value is not a dictionary. Defaults to None.

    Returns:
        The value found at the specified nested path, or the `default` value
        if any part of the path is invalid or not found.

    """
    current_level = d
    for key in keys:
        if isinstance(current_level, dict):
            current_level = current_level.get(key, default)  # type: ignore[assignment]
            if current_level is default and key != keys[-1]:
                return default
        else:
            return default
    return current_level


def resource_in_path(check, resource) -> bool:
    """Validate that a check is applicable to a specific resource path.

    Returns:
        bool: Whether the check is applicable to the resource.

    """
    return object_in_path(check.include, resource.original_file_path) and not (
        check.exclude is not None
        and object_in_path(check.exclude, resource.original_file_path)
    )


def find_missing_meta_keys(meta_config, required_keys) -> list[str]:
    """Find missing keys in a meta config.

    Returns:
        list[str]: List of missing keys.

    """
    # Get all keys in meta config
    keys_in_meta = list(flatten(meta_config).keys())

    # Get required keys and convert to a list
    required_keys = [
        re.sub(r"(\>{1}\d{1,10})", "", f"{k}>{v}")
        for k, v in flatten(required_keys).items()
    ]

    return [
        x
        for x in required_keys
        if (x not in keys_in_meta)
        and (
            x not in [y[:-2] for y in keys_in_meta if y[-3] != ">" and y[-2] == ">"]
        )  # Account for a key with a value that is a list
    ]


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


def _extract_checks_from_module(
    module: Any, module_name: str, check_objects: list[type["BaseCheck"]]
) -> None:
    """Extract Check* classes from a loaded module.

    Inspects the given module and appends any class whose name starts with
    "Check" and is defined within the specified module (not imported from
    elsewhere) to the provided check_objects list.

    Args:
        module: The loaded module object to inspect.
        module_name: The fully qualified name of the module (used to filter
            out imported classes).
        check_objects: A list to which discovered check classes will be
            appended.

    """
    for name, obj in inspect.getmembers(module):
        if (
            inspect.isclass(obj)
            and name.startswith("Check")
            and obj.__module__ == module_name
        ):
            check_objects.append(obj)


def _load_custom_checks(
    custom_checks_dir: Path, check_objects: list[type["BaseCheck"]]
) -> None:
    """Load custom check classes from a directory.

    Scans the specified directory for Python files in subdirectories
    (e.g., `custom_checks_dir/category/check_file.py`) and loads any
    Check* classes defined in them.

    Each file is loaded as a uniquely named module to avoid conflicts with
    internal checks or other custom checks.

    Args:
        custom_checks_dir: Path to the directory containing custom checks.
        check_objects: A list to which discovered check classes will be
            appended.

    Raises:
        RuntimeError: If a custom check file fails to load.

    """
    logging.debug(f"{custom_checks_dir=}")
    if custom_checks_dir.exists():
        custom_check_files = [
            f for f in custom_checks_dir.glob("*/*.py") if f.is_file()
        ]
        logging.debug(f"{custom_check_files=}")

        for check_file in custom_check_files:
            # Use a unique module name to avoid conflicts
            unique_module_name = (
                f"custom_check_{check_file.stem}_{abs(hash(str(check_file)))}"
            )

            try:
                spec = importlib.util.spec_from_file_location(
                    unique_module_name, check_file
                )
                if spec and spec.loader:
                    module = importlib.util.module_from_spec(spec)
                    sys.modules[unique_module_name] = module
                    spec.loader.exec_module(module)

                    _extract_checks_from_module(
                        module, unique_module_name, check_objects
                    )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to load custom check file {check_file}: {e}"
                ) from e
    else:
        logging.warning(
            f"Custom checks directory `{custom_checks_dir}` does not exist."
        )


@lru_cache
def get_check_objects(
    custom_checks_dir: Path | None = None,
) -> list[type["BaseCheck"]]:
    """Get list of Check* classes.

    This function dynamically discovers and loads check classes from two sources:
    1. Internal checks located in the `checks` directory of the package.
    2. Custom checks located in the specified `custom_checks_dir` (if provided).

    It filters for classes that:
    - Start with "Check".
    - Are defined within the loaded module (not imported).

    The result is cached using `@lru_cache` to avoid redundant file scanning
    and module loading on subsequent calls. Import errors in individual files
    are logged as warnings and do not stop execution.

    Args:
        custom_checks_dir: Path to a directory containing custom checks.

    Returns:
        list[type[BaseCheck]]: List of Check* classes.

    """
    check_objects: list[type["BaseCheck"]] = []

    # 1. Load internal checks
    check_files = [
        f for f in (Path(__file__).parent / "checks").glob("*/*.py") if f.is_file()
    ]
    for check_file in check_files:
        # e.g. dbt_bouncer.checks.manifest.check_models
        module_name = ".".join(
            ["dbt_bouncer", "checks", check_file.parts[-2], check_file.stem]
        )
        try:
            module = importlib.import_module(module_name)
            _extract_checks_from_module(module, module_name, check_objects)
        except ImportError:
            logging.warning(f"Failed to import internal check module: {module_name}")

    # 2. Load custom checks (if any)
    if custom_checks_dir is not None:
        _load_custom_checks(Path(custom_checks_dir), check_objects)

    logging.debug(f"Loaded {len(check_objects)} `Check*` classes.")

    return check_objects


def get_clean_model_name(unique_id: str) -> str:
    """`name` for versioned models does not include the version. This function returns the model name concatenated to the version.

    Args:
        unique_id (str): From dbt's artifacts.

    Returns:
        str: The model name including the version number (if present).

    """
    return "_".join(unique_id.split(".")[2:])


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


def is_description_populated(description: str, min_description_length: int) -> bool:
    """Check if a description is populated.

    Args:
        description (str): Description.
        min_description_length (int): Minimum length of the description.

    Returns:
        bool: Whether a description is validly populated.

    """
    return len(
        description.strip()
    ) >= min_description_length and description.strip().lower() not in (
        "none",
        "null",
        "n/a",
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


def make_markdown_table(array: list[list[str]]) -> str:
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


def object_in_path(include_pattern: str | None, path: str) -> bool:
    """Determine if an object is included in the specified path pattern.

    If no pattern is specified then all objects are included.

    Returns:
        bool: True if the object is included in the path pattern, False otherwise.

    """
    if include_pattern is None:
        return True
    return re.compile(include_pattern.strip()).match(clean_path_str(path)) is not None
