import json
import logging
import warnings
from pathlib import Path
from typing import TYPE_CHECKING, List, Literal, Union

from dbt_bouncer.utils import get_package_version_number

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_catalog import DbtBouncerCatalogNode
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        DbtBouncerModel,
        DbtBouncerSemanticModel,
        DbtBouncerSnapshot,
        DbtBouncerSource,
        DbtBouncerTest,
        UnitTests,
    )
    from dbt_bouncer.artifact_parsers.parsers_run_results import (
        DbtBouncerRunResult,
        DbtBouncerRunResultBase,
    )

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogV1

    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
        Exposures,
        Macros,
    )
    from dbt_bouncer.config_file_parser import (
        DbtBouncerConfAllCategories as DbtBouncerConf,
    )


def load_dbt_artifact(
    artifact_name: Literal["catalog.json", "manifest.json", "run_results.json"],
    dbt_artifacts_dir: Path,
) -> Union["CatalogV1", "DbtBouncerManifest", "DbtBouncerRunResultBase"]:
    """Load a dbt artifact from a JSON file to a Pydantic object.

    Returns:
        Union[CatalogV1, DbtBouncerManifest, DbtBouncerRunResultBase]:
            The dbt artifact loaded as a Pydantic object.

    Raises:
        FileNotFoundError:
            If the artifact file does not exist.

    """
    logging.debug(f"{artifact_name=}")
    logging.debug(f"{dbt_artifacts_dir=}")

    artifact_path = dbt_artifacts_dir / Path(artifact_name)
    logging.debug(f"Loading {artifact_name} from {artifact_path}...")
    if not Path(artifact_path).exists():
        raise FileNotFoundError(
            f"No {artifact_name} found at {artifact_path}.",
        )

    if artifact_name == "catalog.json":
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            from dbt_artifacts_parser.parsers.catalog.catalog_v1 import CatalogV1
        with Path.open(Path(artifact_path), "r") as fp:
            catalog_obj = CatalogV1(**json.load(fp))

        return catalog_obj

    elif artifact_name == "manifest.json":
        # First assess dbt version is sufficient
        with Path.open(Path(artifact_path), "r") as fp:
            manifest_json = json.load(fp)

        assert get_package_version_number(
            manifest_json["metadata"]["dbt_version"]
        ) >= get_package_version_number("1.6.0"), (
            f"The supplied `manifest.json` was generated with dbt version {manifest_json['metadata']['dbt_version']}, this is below the minimum supported version of 1.6.0."
        )

        from dbt_bouncer.artifact_parsers.parsers_manifest import (
            DbtBouncerManifest,
            parse_manifest,
        )

        manifest_obj = parse_manifest(manifest_json)
        return DbtBouncerManifest(**{"manifest": manifest_obj})

    elif artifact_name == "run_results.json":
        from dbt_bouncer.artifact_parsers.parsers_run_results import parse_run_results

        with Path.open(Path(artifact_path), "r") as fp:
            run_results_obj = parse_run_results(run_results=json.load(fp))

        return run_results_obj


def parse_dbt_artifacts(
    bouncer_config: "DbtBouncerConf", dbt_artifacts_dir: Path
) -> tuple[
    "DbtBouncerManifest",
    List["Exposures"],
    List["Macros"],
    List["DbtBouncerModel"],
    List["DbtBouncerSemanticModel"],
    List["DbtBouncerSnapshot"],
    List["DbtBouncerSource"],
    List["DbtBouncerTest"],
    List["UnitTests"],
    List["DbtBouncerCatalogNode"],
    List["DbtBouncerCatalogNode"],
    List["DbtBouncerRunResult"],
]:
    """Parse all required dbt artifacts.

    Args:
        bouncer_config (DbtBouncerConf): All checks to be run.
        dbt_artifacts_dir (Path): Path to directory where artifacts are located.

    Returns:
        DbtBouncerManifest: The manifest object.
        List[DbtBouncerExposure]: List of exposures in the project.
        List[DbtBouncerMacro]: List of macros in the project.
        List[DbtBouncerModel]: List of models in the project.
        List[DbtBouncerSemanticModel]: List of semantic models in the project.
        List[DbtBouncerSnapshot]: List of snapshots in the project.
        List[DbtBouncerSource]: List of sources in the project.
        List[DbtBouncerTest]: List of tests in the project.
        List[DbtBouncerUnitTest]: List of unit tests in the project.
        List[DbtBouncerCatalogNode]: List of catalog nodes for the project.
        List[DbtBouncerCatalogNode]: List of catalog nodes for the project sources.
        List[DbtBouncerRunResult]: A list of DbtBouncerRunResult objects.

    """
    from dbt_bouncer.artifact_parsers.parsers_common import load_dbt_artifact
    from dbt_bouncer.artifact_parsers.parsers_manifest import parse_manifest_artifact

    # Manifest, will always be parsed
    manifest_obj = load_dbt_artifact(
        artifact_name="manifest.json",
        dbt_artifacts_dir=dbt_artifacts_dir,
    )
    (
        project_exposures,
        project_macros,
        project_models,
        project_semantic_models,
        project_snapshots,
        project_sources,
        project_tests,
        project_unit_tests,
    ) = parse_manifest_artifact(
        manifest_obj=manifest_obj,
    )

    # Catalog, must come after manifest is parsed
    if (
        hasattr(bouncer_config, "catalog_checks")
        and bouncer_config.catalog_checks != []
    ):
        from dbt_bouncer.artifact_parsers.parsers_catalog import parse_catalog

        project_catalog_nodes, project_catalog_sources = parse_catalog(
            artifact_dir=dbt_artifacts_dir,
            manifest_obj=manifest_obj,
        )
    else:
        project_catalog_nodes = []
        project_catalog_sources = []

    # Run results, must come after manifest is parsed
    if (
        hasattr(bouncer_config, "run_results_checks")
        and bouncer_config.run_results_checks != []
    ):
        from dbt_bouncer.artifact_parsers.parsers_run_results import (
            parse_run_results_artifact,
        )

        project_run_results = parse_run_results_artifact(
            artifact_dir=dbt_artifacts_dir,
            manifest_obj=manifest_obj,
        )
    else:
        project_run_results = []

    return (
        manifest_obj,
        project_exposures,
        project_macros,
        project_models,
        project_semantic_models,
        project_snapshots,
        project_sources,
        project_tests,
        project_unit_tests,
        project_catalog_nodes,
        project_catalog_sources,
        project_run_results,
    )
