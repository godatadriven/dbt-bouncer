"""Re-usable functions for dbt-bouncer."""

import contextlib
import hashlib
import importlib
import importlib.util
import inspect
import logging
import os
import pkgutil
import re
import sys
import typing
from collections.abc import Mapping
from functools import lru_cache
from importlib.metadata import entry_points
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml
from packaging.version import Version as PyPIVersion
from semver import Version

if TYPE_CHECKING:
    from dbt_bouncer.check_framework.base import BaseCheck


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
            current_level = current_level.get(key, default)
            if current_level is default and key != keys[-1]:
                return default
        else:
            return default
    return current_level


def resource_in_path(check: "BaseCheck", resource: Any) -> bool:
    """Validate that a check is applicable to a specific resource path.

    Returns:
        bool: Whether the check is applicable to the resource.

    """
    if not object_in_path(check.include, resource.original_file_path):
        return False
    return not (
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


_SEPARATOR = ">"
_ESCAPED_SEPARATOR = "\\>"


def flatten(
    structure: Any,
    key: str = "",
    path: str = "",
    flattened: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Take a dict of arbitrary depth that may contain lists and return a non-nested dict of all pathways.

    Returns:
        dict[str, Any]: Flattened dict.

    """
    if flattened is None:
        flattened = {}
    if not isinstance(structure, (dict, list)):
        flattened[((path + _SEPARATOR) if path else "") + key] = structure
    elif isinstance(structure, list):
        for i, item in enumerate(structure):
            flatten(item, f"{i}", f"{path}{_SEPARATOR}{key}", flattened)
    else:
        for new_key, value in structure.items():
            # Escape the separator in string keys to prevent path corruption
            escaped_key = (
                new_key.replace(_SEPARATOR, _ESCAPED_SEPARATOR)
                if isinstance(new_key, str)
                else new_key
            )
            flatten(value, escaped_key, f"{path}{_SEPARATOR}{key}", flattened)
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
        Warns if a custom check file fails to load (the file is skipped).

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
            except (
                AttributeError,
                ImportError,
                ModuleNotFoundError,
                OSError,
                SyntaxError,
            ) as e:
                logging.warning(
                    f"Failed to load custom check file `{check_file}`: {e}. "
                    "This file will be skipped."
                )
                logging.debug("Custom check load traceback:", exc_info=True)
    else:
        logging.warning(
            f"Custom checks directory `{custom_checks_dir}` does not exist."
        )


_ENTRY_POINT_GROUP = "dbt_bouncer.checks"


def _load_entry_point_checks(check_objects: list[type["BaseCheck"]]) -> None:
    """Load check classes from installed packages via entry points.

    Scans the ``dbt_bouncer.checks`` entry point group. Each entry point
    can resolve to:

    - A module containing Check* classes (inspected directly)
    - A Check* class (added directly)
    - A package (recursively walked via pkgutil to find Check* classes
      in submodules)

    Args:
        check_objects: A list to which discovered check classes will be
            appended.

    """
    eps = entry_points(group=_ENTRY_POINT_GROUP)
    for ep in eps:
        try:
            target = ep.load()

            if inspect.isclass(target) and target.__name__.startswith("Check"):
                check_objects.append(target)
            elif inspect.ismodule(target):
                # Check if it's a package (has __path__) — walk submodules
                if hasattr(target, "__path__"):
                    for _importer, modname, _ispkg in pkgutil.walk_packages(
                        target.__path__, prefix=f"{target.__name__}."
                    ):
                        try:
                            submodule = importlib.import_module(modname)
                            _extract_checks_from_module(
                                submodule, modname, check_objects
                            )
                        except ImportError:
                            logging.warning(
                                f"Failed to import submodule `{modname}` "
                                f"from entry point `{ep.name}`."
                            )
                            logging.debug("Submodule import traceback:", exc_info=True)
                else:
                    # Plain module — inspect directly
                    _extract_checks_from_module(target, target.__name__, check_objects)
            else:
                logging.warning(
                    f"Entry point `{ep.name}` resolved to "
                    f"{type(target).__name__}, expected a module, package, "
                    "or Check class. Skipping."
                )
        except (AttributeError, ImportError, ModuleNotFoundError) as e:
            logging.warning(
                f"Failed to load entry point `{ep.name}`: {e}. "
                "This plugin will be skipped."
            )
            logging.debug("Entry point load traceback:", exc_info=True)


_CATEGORY_TO_SUBDIR: dict[str, str] = {
    "catalog_checks": "catalog",
    "manifest_checks": "manifest",
    "run_results_checks": "run_results",
}

_SUBDIR_TO_CATEGORY: dict[str, str] = {v: k for k, v in _CATEGORY_TO_SUBDIR.items()}


def _build_check_module_map() -> dict[str, dict[str, str]]:
    """Build a mapping of check_name -> {module, category} by scanning check modules.

    This is the expensive operation that we cache to disk. It imports every
    check module to discover check classes and their names.

    Returns:
        dict[str, dict[str, str]]: Mapping like
            ``{"check_model_names": {"module": "dbt_bouncer.checks.manifest.models.naming", "category": "manifest_checks"}}``.

    """
    checks_dir = Path(__file__).parent / "checks"
    mapping: dict[str, dict[str, str]] = {}

    for check_file in (f for f in checks_dir.glob("**/*.py") if f.is_file()):
        index = check_file.parts.index("checks")
        module_name = ".".join(
            ["dbt_bouncer", "checks", *check_file.parts[index + 1 :]]
        )[:-3]

        # Determine category from path
        relative_parts = check_file.relative_to(checks_dir).parts
        category = (
            _SUBDIR_TO_CATEGORY.get(relative_parts[0], "") if relative_parts else ""
        )

        try:
            module = importlib.import_module(module_name)
        except ImportError:
            continue

        for name, obj in inspect.getmembers(module):
            if (
                inspect.isclass(obj)
                and name.startswith("Check")
                and obj.__module__ == module_name
            ):
                # Extract the Literal name value
                name_field = obj.model_fields.get("name")
                if name_field is not None:
                    args = typing.get_args(name_field.annotation)
                    if args:
                        mapping[args[0]] = {
                            "module": module_name,
                            "category": category,
                        }

    return mapping


def _hash_py_tree(h: "hashlib._Hash", root: Path) -> None:
    """Update ``h`` with relative paths and mtimes of all ``.py`` files under ``root``.

    Args:
        h: A hash object (e.g. ``hashlib.sha256()``) updated in place.
        root: Directory to scan.

    """
    py_files = sorted(f for f in root.glob("**/*.py") if f.is_file())
    for f in py_files:
        h.update(str(f.relative_to(root)).encode())
        h.update(str(f.stat().st_mtime_ns).encode())


@lru_cache(maxsize=1)
def _internal_checks_digest() -> bytes:
    """Return a digest of the packaged ``checks/`` tree.

    Cached for the lifetime of the interpreter — internal check files do not
    change during a single dbt-bouncer invocation, so this avoids re-globbing
    the tree on every cache-key computation. Tests that simulate edits to the
    tree must call ``_internal_checks_digest.cache_clear()`` between runs.

    Returns:
        bytes: The 32-byte SHA-256 digest of the tree.

    """
    h = hashlib.sha256()
    _hash_py_tree(h, Path(__file__).parent / "checks")
    return h.digest()


def _compute_cache_fingerprint(
    version_str: str, custom_checks_dir: Path | None = None
) -> str:
    """Compute a short hash incorporating the version, check files, and entry points.

    The hash covers:
    - The dbt-bouncer version string.
    - Sorted relative paths and modification times of internal ``.py`` files
      in the packaged ``checks/`` directory. This catches editable installs
      where developers add a new check but the version string stays at
      ``"0.0.0"``.
    - Sorted relative paths and modification times of ``.py`` files in
      *custom_checks_dir* (if provided).
    - Sorted entry point names from the ``dbt_bouncer.checks`` group.

    Returns:
        str: An 8-character hex digest suitable for use in a cache filename.

    """
    h = hashlib.sha256(version_str.encode())

    h.update(_internal_checks_digest())

    if custom_checks_dir is not None and custom_checks_dir.exists():
        _hash_py_tree(h, custom_checks_dir)

    ep_names = sorted(ep.name for ep in entry_points(group=_ENTRY_POINT_GROUP))
    for name in ep_names:
        h.update(name.encode())

    return h.hexdigest()[:8]


def compute_conf_cache_key(
    version_str: str,
    config_file_contents: Mapping[str, Any],
    check_categories: list[str],
    custom_checks_dir: Path | None = None,
) -> str:
    """Compute a hash key for the validated bouncer-config cache.

    The key invalidates whenever something that could influence the resulting
    ``DbtBouncerConfBase`` instance changes: the package version, packaged
    check files, custom-check files, installed entry points, the raw config
    contents, the configured categories, or env vars that feed into Pydantic
    ``default_factory`` callables on ``DbtBouncerConfBase``.

    Returns:
        str: 16-character hex digest used to name the cache file.

    """
    import orjson

    h = hashlib.sha256(version_str.encode())

    h.update(_internal_checks_digest())

    if custom_checks_dir is not None and custom_checks_dir.exists():
        _hash_py_tree(h, custom_checks_dir)

    for name in sorted(ep.name for ep in entry_points(group=_ENTRY_POINT_GROUP)):
        h.update(name.encode())

    h.update(orjson.dumps(config_file_contents, option=orjson.OPT_SORT_KEYS))

    for cat in sorted(check_categories):
        h.update(cat.encode())

    # DBT_PROJECT_DIR feeds DbtBouncerConfBase.dbt_artifacts_dir default_factory.
    h.update((os.environ.get("DBT_PROJECT_DIR") or "").encode())

    return h.hexdigest()[:16]


def get_cache_dir() -> Path:
    """Return the on-disk cache directory used by dbt-bouncer.

    Returns:
        Path: ``~/.cache/dbt-bouncer/``.

    """
    return Path.home() / ".cache" / "dbt-bouncer"


def _get_check_module_map_cached(
    custom_checks_dir: Path | None = None,
) -> dict[str, dict[str, str]]:
    """Get the check name -> module mapping, using disk cache.

    The cache is keyed by the dbt-bouncer version, custom check file paths,
    and installed entry point names. Changes to any of these invalidate the
    cache.

    Args:
        custom_checks_dir: Optional path to a directory containing custom
            checks. Included in the cache fingerprint so that adding or
            removing custom check files triggers a rebuild.

    Returns:
        dict[str, dict[str, str]]: The cached mapping.

    """
    import orjson

    from dbt_bouncer.version import version

    ver = version()
    fingerprint = _compute_cache_fingerprint(ver, custom_checks_dir)
    cache_dir = get_cache_dir()
    cache_file = cache_dir / f"check_registry_{ver}_{fingerprint}.json"

    if cache_file.exists():
        try:
            return orjson.loads(cache_file.read_bytes())
        except (orjson.JSONDecodeError, OSError):
            logging.debug("Check registry cache corrupted, rebuilding.", exc_info=True)

    mapping = _build_check_module_map()

    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        cache_file.write_bytes(orjson.dumps(mapping))
        _prune_stale_cache_files(cache_dir, keep=cache_file)
    except OSError:
        pass  # Can't write cache (e.g. read-only filesystem), continue without it

    return mapping


def _prune_stale_cache_files(cache_dir: Path, keep: Path) -> None:
    """Delete other ``check_registry_*.json`` files in ``cache_dir``.

    Why: in editable installs the version is ``"0.0.0"`` and the fingerprint
    component changes whenever check files are touched, so stale files
    accumulate over time without ever being reused.

    Args:
        cache_dir: Directory holding cached registry files.
        keep: The freshly-written cache file to retain.

    """
    for f in cache_dir.glob("check_registry_*.json"):
        if f == keep:
            continue
        with contextlib.suppress(OSError):
            f.unlink()


def get_check_objects_for_names(
    check_names: frozenset[str],
    custom_checks_dir: Path | None = None,
) -> list[type["BaseCheck"]]:
    """Get check classes for a specific set of check names.

    Uses a disk-cached name-to-module mapping to import only the modules that
    contain the requested checks, avoiding the cost of importing all check
    modules.

    Falls back to the full ``get_check_objects()`` scan if the cache is
    unavailable or a requested name is not in the cache.

    Args:
        check_names: The set of check names to load (e.g.
            ``{"check_model_names", "check_model_description_populated"}``).
        custom_checks_dir: Path to a directory containing custom checks.

    Returns:
        list[type[BaseCheck]]: List of check classes for the requested names.

    """
    module_map = _get_check_module_map_cached(custom_checks_dir)

    # Check if all requested names are in the cache
    missing = check_names - module_map.keys()
    if missing:
        logging.debug(
            f"Check names not in cache: {missing}. Falling back to full scan."
        )
        return get_check_objects(custom_checks_dir)

    # Import only the needed modules
    needed_modules: set[str] = {module_map[n]["module"] for n in check_names}
    check_objects: list[type["BaseCheck"]] = []

    for module_name in sorted(needed_modules):
        try:
            module = importlib.import_module(module_name)
            _extract_checks_from_module(module, module_name, check_objects)
        except ImportError:
            logging.warning(f"Failed to import check module: {module_name}")
            logging.debug("Check module import traceback:", exc_info=True)

    # Also load custom checks and entry points
    if custom_checks_dir is not None:
        _load_custom_checks(Path(custom_checks_dir), check_objects)
    _load_entry_point_checks(check_objects)

    # Deduplicate
    seen: set[type] = set()
    unique: list[type["BaseCheck"]] = []
    for cls in check_objects:
        if cls not in seen:
            seen.add(cls)
            unique.append(cls)

    logging.debug(
        f"Loaded {len(unique)} check classes for {len(check_names)} configured checks."
    )
    return unique


@lru_cache
def get_check_objects(
    custom_checks_dir: Path | None = None,
    check_categories: frozenset[str] | None = None,
) -> list[type["BaseCheck"]]:
    """Get list of Check* classes.

    This function dynamically discovers and loads check classes from three
    sources:

    1. Internal checks located in the ``checks`` directory of the package.
    2. Custom checks located in the specified ``custom_checks_dir`` (if
       provided).
    3. Entry-point checks from installed packages registered under the
       ``dbt_bouncer.checks`` entry point group.

    It filters for classes that:
    - Start with "Check".
    - Are defined within the loaded module (not imported).

    Results are deduplicated so a class discovered via multiple paths
    appears only once.

    The result is cached using `@lru_cache` to avoid redundant file scanning
    and module loading on subsequent calls. Import errors in individual files
    are logged as warnings and do not stop execution.

    Args:
        custom_checks_dir: Path to a directory containing custom checks.
        check_categories: Optional frozenset of category names (e.g.
            ``"catalog_checks"``, ``"manifest_checks"``,
            ``"run_results_checks"``) to limit which subdirectories are
            scanned.  When ``None``, all subdirectories are scanned.

    Returns:
        list[type[BaseCheck]]: List of Check* classes.

    """
    check_objects: list[type["BaseCheck"]] = []

    # 1. Load internal checks
    checks_dir = Path(__file__).parent / "checks"

    if check_categories is not None:
        # Only scan the subdirectories that correspond to configured categories
        subdirs = [
            checks_dir / _CATEGORY_TO_SUBDIR[cat]
            for cat in check_categories
            if cat in _CATEGORY_TO_SUBDIR
        ]
        check_files: list[Path] = []
        for subdir in subdirs:
            check_files.extend(f for f in subdir.glob("**/*.py") if f.is_file())
        # Always include top-level .py files (e.g. common.py)
        check_files.extend(f for f in checks_dir.glob("*.py") if f.is_file())
    else:
        check_files = [f for f in checks_dir.glob("**/*.py") if f.is_file()]
    for check_file in check_files:
        index = check_file.parts.index("checks")
        module_name = ".".join(
            ["dbt_bouncer", "checks"] + list(check_file.parts[index + 1 :])  # noqa: RUF005
        )[:-3]  # Remove .py suffix
        try:
            module = importlib.import_module(module_name)
            _extract_checks_from_module(module, module_name, check_objects)
        except ImportError:
            logging.warning(f"Failed to import internal check module: {module_name}")
            logging.debug("Import traceback:", exc_info=True)

    # 2. Load custom checks (if any)
    if custom_checks_dir is not None:
        _load_custom_checks(Path(custom_checks_dir), check_objects)

    # 3. Load entry-point checks (third-party plugins)
    _load_entry_point_checks(check_objects)

    # Deduplicate (same class could be found via internal scan + entry points)
    seen: set[type] = set()
    unique_checks: list[type["BaseCheck"]] = []
    for cls in check_objects:
        if cls not in seen:
            seen.add(cls)
            unique_checks.append(cls)
    check_objects = unique_checks

    logging.debug(f"Loaded {len(check_objects)} `Check*` classes.")

    return check_objects


@lru_cache
def get_check_registry(
    custom_checks_dir: Path | None = None,
) -> dict[str, type["BaseCheck"]]:
    """Return a registry mapping check names to their classes.

    The name used as key is the ``name`` Literal field value on each check class
    (e.g. ``"check_model_access"`` → ``CheckModelAccess``). This enables O(1)
    lookup and is used for validation and error-message generation.

    Args:
        custom_checks_dir: Path to a directory containing custom checks.

    Returns:
        dict[str, type[BaseCheck]]: Mapping of check name → class.

    """
    registry: dict[str, type["BaseCheck"]] = {}
    for cls in get_check_objects(custom_checks_dir):
        # Each check class has a `name: Literal["check_..."]` field annotation.
        # Pydantic stores the Literal type in the annotation; extract the value
        # via typing.get_args since `model_fields["name"].default` is PydanticUndefined.
        name_field = cls.model_fields.get("name")
        if name_field is not None:
            args = typing.get_args(name_field.annotation)
            if args:
                registry[args[0]] = cls
    return registry


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
    if (
        not config_path.exists()
    ):  # Shouldn't be needed as click should have already checked this
        raise FileNotFoundError(f"No config file found at {config_path}.")

    with Path.open(config_path, "r") as f:
        conf = yaml.load(f, Loader=yaml.CSafeLoader)  # type: ignore[possibly-missing-attribute]

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


@lru_cache(maxsize=256)
def compile_pattern(pattern: str, flags: int = 0) -> re.Pattern[str]:
    """Compile and cache a regex pattern.

    Args:
        pattern: The regex pattern string to compile.
        flags: Optional regex flags (e.g. re.DOTALL).

    Returns:
        re.Pattern[str]: The compiled pattern.

    Raises:
        re.error: If the pattern is invalid.

    """
    try:
        return re.compile(pattern, flags)
    except re.error as e:
        raise re.error(f"Invalid regex pattern '{pattern}': {e}") from e


def object_in_path(include_pattern: str | None, path: str) -> bool:
    """Determine if an object is included in the specified path pattern.

    If no pattern is specified then all objects are included.

    Returns:
        bool: True if the object is included in the path pattern, False otherwise.

    """
    if include_pattern is None:
        return True
    return (
        compile_pattern(include_pattern.strip()).match(clean_path_str(path)) is not None
    )
