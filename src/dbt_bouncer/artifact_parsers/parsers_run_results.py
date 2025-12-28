import logging
import warnings
from typing import TYPE_CHECKING, Any, TypeAlias

from pydantic import BaseModel

from dbt_bouncer.utils import clean_path_str

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.run_results.run_results_v5 import (
        RunResultOutput as RunResultOutput_v5,
    )
    from dbt_artifacts_parser.parsers.run_results.run_results_v5 import RunResultsV5

from dbt_bouncer.artifact_parsers.dbt_cloud.run_results_latest import (
    Result as RunResultOutput_Latest,
)
from dbt_bouncer.artifact_parsers.dbt_cloud.run_results_latest import (
    RunResultsLatest,
)

if TYPE_CHECKING:
    from pathlib import Path

    from dbt_bouncer.artifact_parsers.parsers_manifest import DbtBouncerManifest

from dbt_bouncer.artifact_parsers.parsers_common import load_dbt_artifact

DbtBouncerRunResultBase: TypeAlias = RunResultOutput_v5 | RunResultOutput_Latest


class DbtBouncerRunResult(BaseModel):
    """Model for all results in `run_results.json`."""

    original_file_path: str
    run_result: DbtBouncerRunResultBase
    unique_id: str


def parse_run_results(
    run_results: dict[str, Any],
) -> RunResultsV5 | RunResultsLatest:
    """Parse run-results.json.

    Args:
        run_results: A dict of run-results.json

    Raises:
        ValueError: If the schema version is not supported.

    Returns:
        RunResultsV5 | RunResultsLatest:

    """
    dbt_schema_version = run_results["metadata"]["dbt_schema_version"]
    if dbt_schema_version == "https://schemas.getdbt.com/dbt/run-results/v5.json":
        return RunResultsV5(**run_results)
    elif dbt_schema_version == "https://schemas.getdbt.com/dbt/run-results/v6.json":
        return RunResultsLatest(**run_results)
    raise ValueError("Not a manifest.json")


def parse_run_results_artifact(
    artifact_dir: "Path",
    manifest_obj: "DbtBouncerManifest",
    package_name: str | None = None,
) -> list[DbtBouncerRunResult]:
    """Parse the run_results.json artifact.

    Args:
        artifact_dir (Path): Path to the dbt artifacts directory.
        manifest_obj (DbtBouncerManifest): The manifest object.
        package_name (str | None): The package name to filter results by. If None,
            uses the project name from the manifest.

    Returns:
        list[DbtBouncerRunResult]: A list of DbtBouncerRunResult objects.

    Raises:
        TypeError: If the loaded artifact is not of the expected type.

    """

    def get_clean_path_str(unique_id: str, manifest_obj: "DbtBouncerManifest") -> str:
        """Extract the path for multiple node types.

        Args:
            unique_id (str): Id of the node in question
            manifest_obj (DbtBouncerManifest): Manifest

        Returns:
            str: The cleaned path.

        """
        if unique_id in manifest_obj.manifest.nodes:
            return clean_path_str(
                manifest_obj.manifest.nodes[unique_id].original_file_path
            )
        elif unique_id.split(".")[0] == "exposure":
            return clean_path_str(
                manifest_obj.manifest.exposures[unique_id].original_file_path
            )
        else:
            unit_tests = getattr(manifest_obj.manifest, "unit_tests", {})
            if unique_id in unit_tests:
                return clean_path_str(unit_tests[unique_id].original_file_path)
            return ""

    run_results_obj = load_dbt_artifact(
        artifact_name="run_results.json",
        dbt_artifacts_dir=artifact_dir,
    )

    if not isinstance(run_results_obj, (RunResultsV5, RunResultsLatest)):
        raise TypeError(
            f"Expected RunResultsV5 or RunResultsLatest, got {type(run_results_obj)}"
        )

    project_run_results = [
        DbtBouncerRunResult(
            original_file_path=get_clean_path_str(
                unique_id=r.unique_id, manifest_obj=manifest_obj
            ),
            run_result=r,
            unique_id=r.unique_id,
        )
        for r in run_results_obj.results
        if r.unique_id.split(".")[1]
        == (package_name or manifest_obj.manifest.metadata.project_name)
    ]
    logging.info(
        f"Parsed `run_results.json`: {len(project_run_results)} results.",
    )
    return project_run_results
