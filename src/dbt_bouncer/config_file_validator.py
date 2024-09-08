import logging
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any, Dict, Mapping

import toml

from dbt_bouncer.utils import load_config_from_yaml

if TYPE_CHECKING:
    from dbt_bouncer.config_file_parser import DbtBouncerConf


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


def load_config_file_contents(config_file_path: PurePath) -> Mapping[str, Any]:
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
            f"Config file must be either a `pyproject.toml`, `.yaml` or `.yml` file. Got {config_file_path.suffix}."
        )


def validate_conf(config_file_contents: Dict[str, Any]) -> "DbtBouncerConf":
    """Validate the configuration and return the Pydantic model.

    Returns:
        DbtBouncerConf: The validated configuration.

    """
    logging.info("Validating conf...")

    # Rebuild the model to ensure all fields are present
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
            CatalogTable,  # noqa: F401
        )
        from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (
            Exposures,  # noqa: F401
            Macros,  # noqa: F401
            UnitTests,  # noqa: F401
        )

    import dbt_bouncer.checks  # noqa: F401
    from dbt_bouncer.checks.common import NestedDict  # noqa: F401
    from dbt_bouncer.config_file_parser import DbtBouncerConf
    from dbt_bouncer.parsers import (  # noqa: F401
        DbtBouncerCatalogNode,
        DbtBouncerExposureBase,
        DbtBouncerManifest,
        DbtBouncerModel,
        DbtBouncerModelBase,
        DbtBouncerRunResult,
        DbtBouncerRunResultBase,
        DbtBouncerSemanticModel,
        DbtBouncerSemanticModelBase,
        DbtBouncerSource,
        DbtBouncerSourceBase,
        DbtBouncerTest,
        DbtBouncerTestBase,
    )

    DbtBouncerConf.model_rebuild()

    return DbtBouncerConf(**config_file_contents)
