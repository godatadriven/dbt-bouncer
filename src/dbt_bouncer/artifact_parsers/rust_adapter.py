"""Adapter for using the Rust-based JSON parser (dbt_artifacts_rs).

When the ``dbt_artifacts_rs`` extension is installed, this module provides
drop-in replacements for the Pydantic-based artifact parsers.  JSON parsing
happens in Rust (~12x faster), and the returned ``JsonObj`` wrappers support
the same attribute-access API that the check classes expect.
"""

from __future__ import annotations

from enum import Enum
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any

from dbt_bouncer.utils import clean_path_str, get_package_version_number

if TYPE_CHECKING:
    from pathlib import Path

try:
    import dbt_artifacts_rs

    RUST_AVAILABLE = True
except ImportError:
    RUST_AVAILABLE = False


def _wrap(original_file_path: str, unique_id: str, **kwargs: Any) -> SimpleNamespace:
    """Create a lightweight wrapper with the same interface as DbtBouncer* Pydantic models.

    Returns:
        SimpleNamespace: A namespace with original_file_path, unique_id, and any extra kwargs.

    """
    return SimpleNamespace(
        original_file_path=original_file_path, unique_id=unique_id, **kwargs
    )


def parse_manifest_rust(json_str: str) -> Any:
    """Parse a manifest JSON string using the Rust extension.

    Returns:
        Any: A JsonObj that supports attribute access for all manifest fields.

    """
    return dbt_artifacts_rs.parse_manifest_json(json_str)  # type: ignore[attr-defined]


def parse_catalog_rust(json_str: str) -> Any:
    """Parse a catalog JSON string using the Rust extension.

    Returns:
        Any: A JsonObj wrapping the catalog data.

    """
    return dbt_artifacts_rs.parse_catalog_json(json_str)  # type: ignore[attr-defined]


def parse_run_results_rust(json_str: str) -> Any:
    """Parse a run-results JSON string using the Rust extension.

    Returns:
        Any: A JsonObj wrapping the run results data.

    """
    return dbt_artifacts_rs.parse_run_results_json(json_str)  # type: ignore[attr-defined]


def load_manifest_artifact_rust(
    dbt_artifacts_dir: Path, package_name: str | None = None
) -> tuple:
    """Load and parse manifest.json using the Rust extension.

    Returns:
        tuple: Same structure as ``parse_dbt_artifacts`` for manifest data:
            (manifest_obj, exposures, macros, models, seeds, semantic_models,
            snapshots, sources, tests, unit_tests).

    Raises:
        FileNotFoundError: If manifest.json does not exist.
        AssertionError: If dbt version is below 1.7.0.

    """
    artifact_path = dbt_artifacts_dir / "manifest.json"
    if not artifact_path.exists():
        raise FileNotFoundError(f"No manifest.json found at {artifact_path}.")

    json_str = artifact_path.read_text()
    manifest_parsed = parse_manifest_rust(json_str)

    dbt_version = manifest_parsed.metadata.dbt_version
    if not get_package_version_number(dbt_version) >= get_package_version_number(
        "1.7.0"
    ):
        raise AssertionError(
            f"The supplied `manifest.json` was generated with dbt version {dbt_version}, "
            "this is below the minimum supported version of 1.7.0."
        )

    manifest_obj = SimpleNamespace(manifest=manifest_parsed)
    target_package = package_name or manifest_parsed.metadata.project_name

    project_exposures = [
        v
        for _, v in manifest_parsed.exposures.items()
        if v.package_name == target_package
    ]
    project_macros = [
        v for _, v in manifest_parsed.macros.items() if v.package_name == target_package
    ]

    project_models = []
    project_seeds = []
    project_snapshots = []
    project_tests = []

    for k, v in manifest_parsed.nodes.items():
        resource_type_value = (
            v.resource_type.value
            if isinstance(v.resource_type, Enum)
            else v.resource_type
        )
        if v.package_name != target_package:
            continue
        match resource_type_value:
            case "model":
                project_models.append(
                    _wrap(
                        model=v,
                        original_file_path=str(clean_path_str(v.original_file_path)),
                        unique_id=k,
                    )
                )
            case "seed":
                project_seeds.append(
                    _wrap(
                        seed=v,
                        original_file_path=str(clean_path_str(v.original_file_path)),
                        unique_id=k,
                    )
                )
            case "snapshot":
                project_snapshots.append(
                    _wrap(
                        snapshot=v,
                        original_file_path=str(clean_path_str(v.original_file_path)),
                        unique_id=k,
                    )
                )
            case "test":
                project_tests.append(
                    _wrap(
                        test=v,
                        original_file_path=str(clean_path_str(v.original_file_path)),
                        unique_id=k,
                    )
                )

    project_semantic_models = [
        _wrap(
            semantic_model=v,
            original_file_path=str(clean_path_str(v.original_file_path)),
            unique_id=k,
        )
        for k, v in manifest_parsed.semantic_models.items()
        if v.package_name == target_package
    ]
    project_sources = [
        _wrap(
            source=v,
            original_file_path=str(clean_path_str(v.original_file_path)),
            unique_id=k,
        )
        for k, v in manifest_parsed.sources.items()
        if v.package_name == target_package
    ]

    if get_package_version_number(
        manifest_parsed.metadata.dbt_version or "0.0.0"
    ) >= get_package_version_number("1.8.0"):
        project_unit_tests = [
            v
            for _, v in manifest_parsed.unit_tests.items()
            if v.package_name == target_package
        ]
    else:
        project_unit_tests = []

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
    )


def load_catalog_artifact_rust(
    dbt_artifacts_dir: Path,
    manifest_obj: Any,
    package_name: str | None = None,
) -> tuple[list, list]:
    """Load and parse catalog.json using the Rust extension.

    Returns:
        tuple[list, list]: (catalog_nodes, catalog_sources).

    Raises:
        FileNotFoundError: If catalog.json does not exist.

    """
    artifact_path = dbt_artifacts_dir / "catalog.json"
    if not artifact_path.exists():
        raise FileNotFoundError(f"No catalog.json found at {artifact_path}.")

    json_str = artifact_path.read_text()
    catalog_parsed = parse_catalog_rust(json_str)

    target_package = package_name or manifest_obj.manifest.metadata.project_name

    # Catalog nodes — filter by package name in the key (matches standard parser)
    project_catalog_nodes = []
    if catalog_parsed.nodes:
        for k, v in catalog_parsed.nodes.items():
            if k.split(".")[-2] == target_package:
                original_file_path = ""
                if k in manifest_obj.manifest.nodes:
                    original_file_path = str(
                        clean_path_str(
                            manifest_obj.manifest.nodes[k].original_file_path
                        )
                    )
                project_catalog_nodes.append(
                    _wrap(
                        catalog_node=v,
                        original_file_path=original_file_path,
                        unique_id=k,
                    )
                )

    # Catalog sources — filter by package name in the key
    project_catalog_sources = []
    if catalog_parsed.sources:
        for k, v in catalog_parsed.sources.items():
            if k.split(".")[1] == target_package:
                original_file_path = ""
                if k in manifest_obj.manifest.sources:
                    original_file_path = str(
                        clean_path_str(
                            manifest_obj.manifest.sources[k].original_file_path
                        )
                    )
                project_catalog_sources.append(
                    _wrap(
                        catalog_node=v,
                        original_file_path=original_file_path,
                        unique_id=k,
                    )
                )

    return project_catalog_nodes, project_catalog_sources


def load_run_results_artifact_rust(
    dbt_artifacts_dir: Path,
    manifest_obj: Any,
    package_name: str | None = None,
) -> list:
    """Load and parse run_results.json using the Rust extension.

    Returns:
        list: Run result wrappers with original_file_path, unique_id, and run_result attrs.

    Raises:
        FileNotFoundError: If run_results.json does not exist.

    """
    artifact_path = dbt_artifacts_dir / "run_results.json"
    if not artifact_path.exists():
        raise FileNotFoundError(f"No run_results.json found at {artifact_path}.")

    json_str = artifact_path.read_text()
    rr_parsed = parse_run_results_rust(json_str)

    target_package = package_name or manifest_obj.manifest.metadata.project_name

    def _get_clean_path(unique_id: str) -> str:
        if unique_id in manifest_obj.manifest.nodes:
            return str(
                clean_path_str(
                    manifest_obj.manifest.nodes[unique_id].original_file_path
                )
            )
        elif unique_id.split(".")[0] == "exposure":
            return str(
                clean_path_str(
                    manifest_obj.manifest.exposures[unique_id].original_file_path
                )
            )
        else:
            unit_tests = getattr(manifest_obj.manifest, "unit_tests", {})
            if unique_id in unit_tests:
                return str(clean_path_str(unit_tests[unique_id].original_file_path))
            return ""

    project_run_results = [
        _wrap(
            run_result=r,
            original_file_path=_get_clean_path(r.unique_id),
            unique_id=r.unique_id,
        )
        for r in rr_parsed.results
        if r.unique_id.split(".")[1] == target_package
    ]

    return project_run_results
