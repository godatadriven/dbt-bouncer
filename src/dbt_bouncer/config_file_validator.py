import logging
import os
import re
import tomllib
from collections.abc import Mapping
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

import jellyfish
import typer
import yaml
from pydantic import ValidationError

from dbt_bouncer.enums import ConfigFileName
from dbt_bouncer.utils import compile_pattern, get_check_registry, load_config_from_yaml

if TYPE_CHECKING:
    from dbt_bouncer.config_file_parser import DbtBouncerConfBase

_rebuilt_classes: set[int] = set()

DEFAULT_DBT_BOUNCER_CONFIG = """manifest_checks:
  - name: check_model_directories
    include: ^models
    permitted_sub_directories:
      - intermediate
      - marts
      - staging
  - name: check_model_names
    include: ^models/staging
    model_name_pattern: ^stg_
"""


def get_config_file_path(
    config_file: PurePath,
    config_file_source: str,
) -> PurePath:
    """Get the path to the config file for dbt-bouncer. This is fetched from (in order):

    1. The file passed via the `--config-file` CLI flag.
    2. The file passed via the `DBT_BOUNCER_CONFIG_FILE` environment variable.
    3. A file named `dbt-bouncer.yml` in the current working directory.
    4. A file named `dbt-bouncer.toml` in the current working directory.
    5. A `[tool.dbt-bouncer]` section in `pyproject.toml` (in current working directory or parent directories).

    Returns:
        PurePath: Config file for dbt-bouncer.

    Raises:
        RuntimeError: If no config file is found.

    """  # noqa: D400, D415
    logging.debug(f"{config_file=}")
    logging.debug(f"{config_file_source=}")

    if config_file_source == "COMMANDLINE":
        logging.debug(f"Config file passed via command line: {config_file}")
        config_file_path = Path(config_file)
        if not config_file_path.exists():
            raise RuntimeError(f"Config file not found: {config_file}")
        return config_file

    if config_file_path_via_env_var := os.getenv("DBT_BOUNCER_CONFIG_FILE"):
        logging.debug(
            f"Config file passed via environment variable: {config_file_path_via_env_var}"
        )
        return Path(config_file_path_via_env_var)

    if config_file_source == "DEFAULT":
        logging.debug(f"Using default value for config file: {config_file}")
        config_file_path = Path.cwd() / config_file
        if config_file_path.exists():
            return config_file_path

    # Check for dbt-bouncer.toml in the current working directory
    toml_config_path = Path.cwd() / ConfigFileName.DBT_BOUNCER_TOML
    if toml_config_path.exists():
        logging.debug(f"Found dbt-bouncer.toml: {toml_config_path}")
        return toml_config_path

    # Read config from pyproject.toml
    logging.debug("Loading config from pyproject.toml, if exists...")
    if (Path().cwd() / ConfigFileName.PYPROJECT_TOML).exists():
        pyproject_toml_dir = Path().cwd()
    else:
        pyproject_toml_dir = next(
            (
                parent
                for parent in Path().cwd().parents
                if (parent / ConfigFileName.PYPROJECT_TOML).exists()
            ),
            None,
        )  # i.e. look in parent directories for a pyproject.toml file

        if pyproject_toml_dir is None:
            logging.debug("No pyproject.toml found.")
            raise RuntimeError(
                "No config file found. Please provide a `dbt-bouncer.yml`, `dbt-bouncer.toml`, or a `pyproject.toml` with a `[tool.dbt-bouncer]` section. Alternatively, pass the path via the `--config-file` flag.",
            )

    return pyproject_toml_dir / ConfigFileName.PYPROJECT_TOML


def load_config_file_contents(
    config_file_path: PurePath,
    allow_default_config_file_creation: bool | None = None,
) -> Mapping[str, Any]:
    """Load the contents of the config file.

    Args:
        config_file_path: Path to the config file.
        allow_default_config_file_creation: Whether to allow the creation of a default config file if one does not exist. Used to allow pytesting of this function.

    Returns:
        Mapping[str, Any]: Config for dbt-bouncer.

    Raises:
        RuntimeError: If the config file type is not supported or does not contain the expected keys.

    """
    match config_file_path.suffix:
        case ".yml" | ".yaml":
            return load_config_from_yaml(Path(config_file_path))
        case ".toml":
            with Path(config_file_path).open("rb") as f:
                toml_cfg = tomllib.load(f)

            # dbt-bouncer.toml: config is at the top level
            if config_file_path.name == ConfigFileName.DBT_BOUNCER_TOML:
                return toml_cfg

            # pyproject.toml: config is under [tool.dbt-bouncer]
            if toml_cfg.get("tool", {}).get("dbt-bouncer"):
                return toml_cfg["tool"]["dbt-bouncer"]
            else:
                logging.warning(
                    "Cannot find a `dbt-bouncer.yml` file, `dbt-bouncer.toml` file, or a `[tool.dbt-bouncer]` section in pyproject.toml."
                )
                if (
                    allow_default_config_file_creation is True
                    and os.getenv("CREATE_DBT_BOUNCER_CONFIG_FILE") != "false"
                    and (
                        os.getenv("CREATE_DBT_BOUNCER_CONFIG_FILE") == "true"
                        or typer.confirm(
                            "Do you want `dbt-bouncer` to create a `dbt-bouncer.yml` file in the current directory?"
                        )
                    )
                ):
                    created_config_file = Path.cwd().joinpath(
                        ConfigFileName.DBT_BOUNCER_YML
                    )
                    created_config_file.touch()
                    logging.info(
                        "A `dbt-bouncer.yml` file has been created in the current directory with default settings."
                    )
                    with Path.open(created_config_file, "w") as f:
                        f.write(DEFAULT_DBT_BOUNCER_CONFIG)

                    return load_config_from_yaml(created_config_file)

                else:
                    raise RuntimeError(
                        "No configuration for `dbt-bouncer` could be found. You can pass the path to your config file via the `--config-file` flag. Alternatively, configure `pyproject.toml` or use a `dbt-bouncer.toml` file.",
                    )
        case _:
            raise RuntimeError(
                f"Config file must be a `.toml`, `.yaml`, or `.yml` file. Got {config_file_path.suffix}."
            )


def lint_config_file(config_file_path: Path) -> list[dict[str, Any]]:
    """Lint the config file and return a list of issues with line numbers.

    Args:
        config_file_path: Path to the config file.

    Returns:
        list[dict[str, Any]]: List of issues found, each with 'line', 'message', and 'severity'.

    """
    issues: list[dict[str, Any]] = []

    if config_file_path.suffix not in [".yml", ".yaml"]:
        return issues

    try:
        content = config_file_path.read_text()
        data = yaml.load(content, Loader=yaml.CSafeLoader)  # type: ignore[possibly-missing-attribute]
    except yaml.YAMLError as e:
        problem_mark = getattr(e, "problem_mark", None)
        if problem_mark:
            issues.append(
                {
                    "line": problem_mark.line + 1,
                    "message": f"YAML syntax error: {getattr(e, 'problem', str(e))}",
                    "severity": "error",
                }
            )
        else:
            issues.append(
                {
                    "line": 1,
                    "message": f"YAML syntax error: {e}",
                    "severity": "error",
                }
            )
        return issues
    except Exception:
        return issues

    if not data:
        issues.append(
            {
                "line": 1,
                "message": "Config file is empty",
                "severity": "error",
            }
        )
        return issues

    registry = get_check_registry()
    valid_categories = {"manifest_checks", "catalog_checks", "run_results_checks"}
    for category in valid_categories:
        if category in data:
            checks = data[category]
            if not isinstance(checks, list):
                issues.append(
                    {
                        "line": 1,
                        "message": f"'{category}' must be a list, got {type(checks).__name__}",
                        "severity": "error",
                    }
                )
                continue

            for idx, check in enumerate(checks):
                if not isinstance(check, dict):
                    issues.append(
                        {
                            "line": idx + 1,
                            "message": f"Check must be a dictionary, got {type(check).__name__}",
                            "severity": "error",
                        }
                    )
                    continue

                if "name" not in check:
                    issues.append(
                        {
                            "line": idx + 1,
                            "message": "Check is missing required 'name' field",
                            "severity": "error",
                        }
                    )
                    continue  # Cannot validate the name if it's absent

                check_name = check["name"]
                if check_name not in registry:
                    best_match = min(
                        registry.keys(),
                        key=lambda k: jellyfish.levenshtein_distance(k, check_name),
                        default=None,
                    )
                    suggestion = f" Did you mean '{best_match}'?" if best_match else ""
                    issues.append(
                        {
                            "line": idx + 1,
                            "message": f"Unknown check name '{check_name}'.{suggestion}",
                            "severity": "error",
                        }
                    )

    return issues


def _get_stub_namespace() -> dict[str, Any]:
    """Return a lightweight namespace for Pydantic model_rebuild() during config validation.

    ``NestedDict`` is the only forward reference used in check field annotations
    that must resolve to a real class. All artifact wrapper types now use ``Any``
    directly in ``BaseCheck``, so no stubs are needed for them.

    Returns:
        dict[str, Any]: Namespace mapping type names for ``model_rebuild()``.

    """
    from dbt_bouncer.checks.common import NestedDict

    return {"NestedDict": NestedDict}


def validate_conf(
    check_categories,  #: list[Literal["catalog_checks"], Literal["manifest_checks"], Literal["run_results_checks"]],
    config_file_contents: dict[str, Any],
    custom_checks_dir: Path | None = None,
) -> "DbtBouncerConfBase":
    """Validate the configuration and return the Pydantic model.

    Returns:
        DbtBouncerConf: The validated configuration.

    Raises:
        RuntimeError: If the configuration is invalid.

    """
    logging.info("Validating conf...")

    # Extract check names from config to enable targeted module loading.
    configured_check_names: set[str] = set()
    for cat in check_categories:
        for entry in config_file_contents.get(cat, []):
            if isinstance(entry, dict) and "name" in entry:
                configured_check_names.add(entry["name"])

    if configured_check_names:
        # Fast path: import only modules containing the configured checks.
        from dbt_bouncer.config_file_parser import _create_conf_class
        from dbt_bouncer.utils import get_check_objects_for_names

        check_objects = get_check_objects_for_names(
            frozenset(configured_check_names),
            custom_checks_dir=custom_checks_dir,
        )
        DbtBouncerConf = _create_conf_class(  # noqa: N806
            custom_checks_dir=custom_checks_dir,
            check_categories=frozenset(check_categories),
            check_objects=check_objects,
        )
    else:
        # Fallback: no check names to extract, use full scan.
        if "catalog_checks" in check_categories:
            import dbt_bouncer.checks.catalog
        if "manifest_checks" in check_categories:
            import dbt_bouncer.checks.manifest
        if "run_results_checks" in check_categories:
            import dbt_bouncer.checks.run_results  # noqa: F401

        from dbt_bouncer.config_file_parser import create_bouncer_conf_class

        DbtBouncerConf = create_bouncer_conf_class(  # noqa: N806
            custom_checks_dir=custom_checks_dir,
            check_categories=frozenset(check_categories),
        )
    if id(DbtBouncerConf) not in _rebuilt_classes:
        DbtBouncerConf.model_rebuild(_types_namespace=_get_stub_namespace())
        _rebuilt_classes.add(id(DbtBouncerConf))

    try:
        return DbtBouncerConf(**config_file_contents)
    except ValidationError as e:
        accepted_names = list(get_check_registry(custom_checks_dir).keys())
        error_message: list[str] = []
        for error in e.errors():
            if (
                compile_pattern(
                    r"Input tag \S* found using 'name' does not match any of the expected tags: [\S\s]*",
                    flags=re.DOTALL,
                ).match(error["msg"])
                is not None
            ):
                incorrect_name = error["msg"][
                    error["msg"].find("tag") + 5 : error["msg"].find("found using") - 2
                ]
                min_dist = 100
                for name in accepted_names:
                    dist = jellyfish.levenshtein_distance(name, incorrect_name)
                    if dist < min_dist:
                        min_dist = dist
                        min_name = name
                error_message.append(
                    f"{len(error_message) + 1}. Check '{incorrect_name}' does not match any of the expected checks. Did you mean '{min_name}'?"
                )

        raise RuntimeError("\n".join(error_message)) from e
