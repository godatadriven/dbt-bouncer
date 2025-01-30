import logging
import os
import re
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any, Dict, List, Literal, Mapping, Optional

import click
import toml
from pydantic import ValidationError

from dbt_bouncer.utils import load_config_from_yaml

if TYPE_CHECKING:
    from dbt_bouncer.config_file_parser import (
        DbtBouncerConfAllCategories as DbtBouncerConf,
    )

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


def conf_cls_factory(
    check_categories: List[
        Literal["catalog_checks", "manifest_checks", "run_results_checks"]
    ],
):
    """Return the appropriate configuration class based on the check categories.

    Args:
        check_categories: List[Literal["catalog_checks", "manifest_checks", "run_results_checks"]]

    Raises:
        ValueError: If the check categories are not valid.

    Returns:
        DbtBouncerConf: The configuration class.

    """
    check_categories = sorted(check_categories)
    if check_categories == ["catalog_checks"]:
        from dbt_bouncer.config_file_parser import DbtBouncerConfCatalogOnly

        return DbtBouncerConfCatalogOnly
    elif check_categories == ["manifest_checks"]:
        from dbt_bouncer.config_file_parser import DbtBouncerConfManifestOnly

        return DbtBouncerConfManifestOnly
    elif check_categories == ["run_results_checks"]:
        from dbt_bouncer.config_file_parser import DbtBouncerConfRunResultsOnly

        return DbtBouncerConfRunResultsOnly
    elif check_categories == ["catalog_checks", "manifest_checks"]:
        from dbt_bouncer.config_file_parser import DbtBouncerConfCatalogManifest

        return DbtBouncerConfCatalogManifest
    elif check_categories == ["catalog_checks", "run_results_checks"]:
        from dbt_bouncer.config_file_parser import DbtBouncerConfCatalogRunResults

        return DbtBouncerConfCatalogRunResults
    elif check_categories == ["manifest_checks", "run_results_checks"]:
        from dbt_bouncer.config_file_parser import DbtBouncerConfManifestRunResults

        return DbtBouncerConfManifestRunResults
    elif check_categories == [
        "catalog_checks",
        "manifest_checks",
        "run_results_checks",
    ]:
        from dbt_bouncer.config_file_parser import DbtBouncerConfAllCategories

        return DbtBouncerConfAllCategories
    else:
        raise ValueError(f"Invalid check_categories: {check_categories}")


def get_config_file_path(
    config_file: PurePath,
    config_file_source: str,
) -> PurePath:
    """Get the path to the config file for dbt-bouncer. This is fetched from (in order):

    1. The file passed via the `--config-file` CLI flag.
    2. A file named `dbt-bouncer.yml` in the current working directory.
    3. A `[tool.dbt-bouncer]` section in `pyproject.toml` (in current working directory or parent directories).

    Returns:
        PurePath: Config file for dbt-bouncer.

    Raises:
        RuntimeError: If no config file is found.

    """  # noqa: D400, D415
    logging.debug(f"{config_file=}")
    logging.debug(f"{config_file_source=}")

    if config_file_source == "COMMANDLINE":
        logging.debug(f"Config file passed via command line: {config_file}")
        return config_file

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


def load_config_file_contents(
    config_file_path: PurePath,
    allow_default_config_file_creation: Optional[bool] = None,
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
    if config_file_path.suffix in [".yml", ".yaml"]:
        return load_config_from_yaml(config_file_path)
    elif config_file_path.suffix in [".toml"]:
        toml_cfg = toml.load(config_file_path)
        if "dbt-bouncer" in toml_cfg["tool"]:
            return next(v for k, v in toml_cfg["tool"].items() if k == "dbt-bouncer")
        else:
            logging.warning(
                "Cannot find a `dbt-bouncer.yml` file or a `dbt-bouncer` section found in pyproject.toml."
            )
            if (
                allow_default_config_file_creation is True
                and os.getenv("CREATE_DBT_BOUNCER_CONFIG_FILE") != "false"
                and (
                    os.getenv("CREATE_DBT_BOUNCER_CONFIG_FILE") == "true"
                    or click.confirm(
                        "Do you want `dbt-bouncer` to create a `dbt-bouncer.yml` file in the current directory?"
                    )
                )
            ):
                created_config_file = Path.cwd().joinpath("dbt-bouncer.yml")
                created_config_file.touch()
                logging.info(
                    "A `dbt-bouncer.yml` file has been created in the current directory with default settings."
                )
                with Path.open(created_config_file, "w") as f:
                    f.write(DEFAULT_DBT_BOUNCER_CONFIG)

                return load_config_from_yaml(created_config_file)

            else:
                raise RuntimeError(
                    "No configuration for `dbt-bouncer` could be found. You can pass the path to your config file via the `--config-file` flag. Alternatively, your pyproject.toml file can be configured to work with `dbt-bouncer`.",
                )
    else:
        raise RuntimeError(
            f"Config file must be either a `pyproject.toml`, `.yaml` or `.yml` file. Got {config_file_path.suffix}."
        )


def validate_conf(
    check_categories,  #: List[Literal["catalog_checks"], Literal["manifest_checks"], Literal["run_results_checks"]],
    config_file_contents: Dict[str, Any],
) -> "DbtBouncerConf":
    """Validate the configuration and return the Pydantic model.

    Raises:
        RuntimeError: If the configuration is invalid.

    Returns:
        DbtBouncerConf: The validated configuration.

    """
    logging.info("Validating conf...")

    # Rebuild the model to ensure all fields are present
    from dbt_bouncer.checks.common import NestedDict  # noqa: F401

    if "catalog_checks" in check_categories:
        import warnings

        import dbt_bouncer.checks.catalog
        from dbt_bouncer.artifact_parsers.parsers_catalog import (  # noqa: F401
            DbtBouncerCatalogNode,
        )
        from dbt_bouncer.artifact_parsers.parsers_manifest import (
            DbtBouncerModelBase,
            DbtBouncerSnapshotBase,
            DbtBouncerSourceBase,
            DbtBouncerTestBase,
        )

        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
                Nodes as CatalogNodes,  # noqa: F401
            )
    if "manifest_checks" in check_categories:
        import dbt_bouncer.checks.manifest
        from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
            Exposures,  # noqa: F401
            Macros,  # noqa: F401
            UnitTests,  # noqa: F401
        )
        from dbt_bouncer.artifact_parsers.parsers_manifest import (  # noqa: F401
            DbtBouncerExposureBase,
            DbtBouncerManifest,
            DbtBouncerModel,
            DbtBouncerModelBase,
            DbtBouncerSemanticModel,
            DbtBouncerSemanticModelBase,
            DbtBouncerSnapshot,
            DbtBouncerSnapshotBase,
            DbtBouncerSource,
            DbtBouncerSourceBase,
            DbtBouncerTest,
            DbtBouncerTestBase,
        )
    if "run_results_checks" in check_categories:
        import dbt_bouncer.checks.run_results  # noqa: F401
        from dbt_bouncer.artifact_parsers.parsers_run_results import (  # noqa: F401
            DbtBouncerRunResult,
            DbtBouncerRunResultBase,
        )

    DbtBouncerConf = conf_cls_factory(check_categories=check_categories)  # noqa: N806
    DbtBouncerConf().model_rebuild()

    try:
        return DbtBouncerConf(**config_file_contents)
    except ValidationError as e:
        from Levenshtein import distance

        error_message: List[str] = []
        for error in e.errors():
            if (
                re.compile(
                    r"Input tag \S* found using 'name' does not match any of the expected tags: [\S\s]*",
                    flags=re.DOTALL,
                ).match(error["msg"])
                is not None
            ):
                incorrect_name = error["msg"][
                    error["msg"].find("tag") + 5 : error["msg"].find("found using") - 2
                ]
                accepted_names = error["msg"][
                    error["msg"].find("tags:") + 7 : -1
                ].split("', '")
                min_dist = 100
                for name in accepted_names:
                    dist = distance(name, incorrect_name)
                    if dist < min_dist:
                        min_dist = dist
                        min_name = name
                error_message.append(
                    f"{len(error_message) + 1}. Check '{incorrect_name}' does not match any of the expected checks. Did you mean '{min_name}'?"
                )

        raise RuntimeError("\n".join(error_message)) from e
