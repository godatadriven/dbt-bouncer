import logging
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import orjson

from dbt_bouncer.utils import get_package_version_number

if TYPE_CHECKING:
    from dbt_artifacts_parser.parsers.run_results.run_results_v5 import RunResultsV5

    from dbt_bouncer.artifact_parsers.dbt_cloud.run_results_latest import (
        RunResultsLatest,
    )
    from dbt_bouncer.artifact_parsers.parsers_catalog import (
        DbtBouncerCatalog,
        DbtBouncerCatalogNode,
    )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerExposureBase,
        DbtBouncerMacroBase,
        DbtBouncerManifest,
        DbtBouncerModel,
        DbtBouncerSeed,
        DbtBouncerSemanticModel,
        DbtBouncerSnapshot,
        DbtBouncerSource,
        DbtBouncerTest,
        UnitTests,
    )
    from dbt_bouncer.artifact_parsers.parsers_run_results import (
        DbtBouncerRunResult,
    )
    from dbt_bouncer.config_file_parser import DbtBouncerConfBase


def load_dbt_artifact(
    artifact_name: Literal["catalog.json", "manifest.json", "run_results.json"],
    dbt_artifacts_dir: Path,
) -> "DbtBouncerCatalog | DbtBouncerManifest | RunResultsV5 | RunResultsLatest":
    """Load a dbt artifact from a JSON file to a Pydantic object.

    Returns:
        DbtBouncerCatalog | DbtBouncerManifest | RunResultsV5 | RunResultsLatest:
            The dbt artifact loaded as a Pydantic object.

    Raises:
        AssertionError:
            If the dbt version is below the minimum supported version of 1.7.0.
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

    artifact_bytes = Path(artifact_path).read_bytes()

    if artifact_name == "catalog.json":
        from dbt_bouncer.artifact_parsers.parsers_catalog import DbtBouncerCatalog

        catalog_obj = DbtBouncerCatalog(**{"catalog": orjson.loads(artifact_bytes)})
        return catalog_obj

    elif artifact_name == "manifest.json":
        manifest_json = orjson.loads(artifact_bytes)

        if not get_package_version_number(
            manifest_json["metadata"]["dbt_version"]
        ) >= get_package_version_number("1.7.0"):
            raise AssertionError(
                f"The supplied `manifest.json` was generated with dbt version {manifest_json['metadata']['dbt_version']}, this is below the minimum supported version of 1.7.0."
            )

        from dbt_bouncer.artifact_parsers.parsers_manifest import (
            DbtBouncerManifest,
            parse_manifest,
        )

        manifest_obj = parse_manifest(manifest_json)
        return DbtBouncerManifest(**{"manifest": manifest_obj})

    elif artifact_name == "run_results.json":
        from dbt_bouncer.artifact_parsers.parsers_run_results import (
            parse_run_results,
        )

        run_results_obj = parse_run_results(run_results=orjson.loads(artifact_bytes))
        return run_results_obj


def parse_dbt_artifacts(
    bouncer_config: "DbtBouncerConfBase", dbt_artifacts_dir: Path
) -> tuple[
    "DbtBouncerManifest",
    list["DbtBouncerExposureBase"],
    list["DbtBouncerMacroBase"],
    list["DbtBouncerModel"],
    list["DbtBouncerSeed"],
    list["DbtBouncerSemanticModel"],
    list["DbtBouncerSnapshot"],
    list["DbtBouncerSource"],
    list["DbtBouncerTest"],
    list["UnitTests"],
    list["DbtBouncerCatalogNode"],
    list["DbtBouncerCatalogNode"],
    list["DbtBouncerRunResult"],
]:
    """Parse all required dbt artifacts.

    Args:
        bouncer_config (DbtBouncerConfBase): All checks to be run.
        dbt_artifacts_dir (Path): Path to directory where artifacts are located.

    Returns:
        DbtBouncerManifest: The manifest object.
        list[DbtBouncerExposure]: List of exposures in the project.
        list[DbtBouncerMacro]: List of macros in the project.
        list[DbtBouncerModel]: List of models in the project.
        list[DbtBouncerSeed]: List of seeds in the project.
        list[DbtBouncerSemanticModel]: List of semantic models in the project.
        list[DbtBouncerSnapshot]: List of snapshots in the project.
        list[DbtBouncerSource]: List of sources in the project.
        list[DbtBouncerTest]: List of tests in the project.
        list[DbtBouncerUnitTest]: List of unit tests in the project.
        list[DbtBouncerCatalogNode]: List of catalog nodes for the project.
        list[DbtBouncerCatalogNode]: List of catalog nodes for the project sources.
        list[DbtBouncerRunResult]: A list of DbtBouncerRunResult objects.

    Raises:
        TypeError: If any of the loaded artifacts are not of the expected type.

    """
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        parse_manifest_artifact,
    )

    # Manifest, will always be parsed
    manifest_obj = load_dbt_artifact(
        artifact_name="manifest.json",
        dbt_artifacts_dir=dbt_artifacts_dir,
    )
    if not isinstance(manifest_obj, DbtBouncerManifest):
        raise TypeError(f"Expected DbtBouncerManifest, got {type(manifest_obj)}")

    (
        project_exposures,
        project_macros,
        project_models,
        project_seeds,
        project_semantic_models,
        project_snapshots,
        project_sources,
        project_tests,
        project_unit_tests,
    ) = parse_manifest_artifact(
        manifest_obj=manifest_obj, package_name=bouncer_config.package_name
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
            package_name=bouncer_config.package_name,
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
            package_name=bouncer_config.package_name,
        )
    else:
        project_run_results = []

    import io
    import logging

    from rich.console import Console
    from rich.table import Table

    project_name = bouncer_config.package_name or manifest_obj.manifest.metadata.project_name
    table = Table(title=None, show_header=True, header_style="bold")
    table.add_column("Artifact", justify="left")
    table.add_column("Category", justify="left")
    table.add_column("Count", justify="right")

    table.add_row("manifest.json", "Exposures", str(len(project_exposures)))
    table.add_row("", "Macros", str(len(project_macros)))
    table.add_row("", "Nodes", str(len(project_models)))
    table.add_row("", "Seeds", str(len(project_seeds)))
    table.add_row("", "Semantic Models", str(len(project_semantic_models)))
    table.add_row("", "Snapshots", str(len(project_snapshots)))
    table.add_row("", "Sources", str(len(project_sources)))
    table.add_row("", "Tests", str(len(project_tests)))
    table.add_row("", "Unit Tests", str(len(project_unit_tests)))

    if hasattr(bouncer_config, "catalog_checks") and bouncer_config.catalog_checks != []:
        table.add_row("catalog.json", "Nodes", str(len(project_catalog_nodes)))
        table.add_row("", "Sources", str(len(project_catalog_sources)))

    if hasattr(bouncer_config, "run_results_checks") and bouncer_config.run_results_checks != []:
        table.add_row("run_results.json", "Results", str(len(project_run_results)))

    console = Console(file=io.StringIO(), force_terminal=False)
    console.print(table)
    table_str = console.file.getvalue()

    logging.info(
        f"Parsed artifacts for `{project_name}` project:\n{table_str}",
    )

    return (
        manifest_obj,
        project_exposures,
        project_macros,
        project_models,
        project_seeds,
        project_semantic_models,
        project_snapshots,
        project_sources,
        project_tests,
        project_unit_tests,
        project_catalog_nodes,
        project_catalog_sources,
        project_run_results,
    )
