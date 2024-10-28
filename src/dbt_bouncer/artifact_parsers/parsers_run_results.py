import logging
import warnings
from typing import TYPE_CHECKING, Any, Dict, List, Union

from pydantic import BaseModel

from dbt_bouncer.utils import clean_path_str

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.run_results.run_results_v4 import (
        RunResultOutput as RunResultOutput_v4,
    )
    from dbt_artifacts_parser.parsers.run_results.run_results_v4 import RunResultsV4
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

DbtBouncerRunResultBase = Union[
    RunResultOutput_v4, RunResultOutput_v5, RunResultOutput_Latest
]


class DbtBouncerRunResult(BaseModel):
    """Model for all results in `run_results.json`."""

    original_file_path: str
    run_result: DbtBouncerRunResultBase
    unique_id: str


def parse_run_results(
    run_results: Dict[str, Any],
) -> Union[RunResultsV4, RunResultsV5, RunResultsLatest]:
    """Parse run-results.json.

    Args:
        run_results: A dict of run-results.json

    Raises:
        ValueError: If the schema version is not supported.

    Returns:
        Union[RunResultsV4, RunResultsV5, RunResultsLatest]:

    """
    dbt_schema_version = run_results["metadata"]["dbt_schema_version"]
    if dbt_schema_version == "https://schemas.getdbt.com/dbt/run-results/v4.json":
        return RunResultsV4(**run_results)
    elif dbt_schema_version == "https://schemas.getdbt.com/dbt/run-results/v5.json":
        return RunResultsV5(**run_results)
    elif dbt_schema_version == "https://schemas.getdbt.com/dbt/run-results/v6.json":
        return RunResultsLatest(**run_results)
    raise ValueError("Not a manifest.json")


def parse_run_results_artifact(
    artifact_dir: "Path",
    manifest_obj: "DbtBouncerManifest",
) -> List[DbtBouncerRunResult]:
    """Parse the run_results.json artifact.

    Returns:
        List[DbtBouncerRunResult]: A list of DbtBouncerRunResult objects.

    """
    run_results_obj: Union[RunResultsV4, RunResultsV5, RunResultsLatest] = (
        load_dbt_artifact(
            artifact_name="run_results.json",
            dbt_artifacts_dir=artifact_dir,
        )
    )

    project_run_results = [
        DbtBouncerRunResult(
            **{
                "original_file_path": (
                    clean_path_str(
                        manifest_obj.manifest.nodes[r.unique_id].original_file_path
                    )
                    if r.unique_id in manifest_obj.manifest.nodes
                    else clean_path_str(
                        manifest_obj.manifest.unit_tests[r.unique_id].original_file_path
                    )
                ),
                "run_result": r,
                "unique_id": r.unique_id,
            },
        )
        for r in run_results_obj.results
        if r.unique_id.split(".")[1] == manifest_obj.manifest.metadata.project_name
    ]
    logging.info(
        f"Parsed `run_results.json`: {len(project_run_results)} results.",
    )
    return project_run_results
