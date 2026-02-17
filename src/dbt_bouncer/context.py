"""A context object to hold all the data needed for a bouncer run."""

from pathlib import Path
from typing import TYPE_CHECKING

from pydantic import BaseModel

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import UnitTests
    from dbt_bouncer.artifact_parsers.parsers_catalog import DbtBouncerCatalogNode
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
    )
    from dbt_bouncer.artifact_parsers.parsers_run_results import DbtBouncerRunResult
    from dbt_bouncer.config_file_parser import DbtBouncerConfBase


class BouncerContext(BaseModel):
    """A context object to hold all the data needed for a bouncer run."""

    bouncer_config: "DbtBouncerConfBase"
    catalog_nodes: list["DbtBouncerCatalogNode"]
    catalog_sources: list["DbtBouncerCatalogNode"]
    check_categories: list[str]
    create_pr_comment_file: bool
    exposures: list["DbtBouncerExposureBase"]
    macros: list["DbtBouncerMacroBase"]
    manifest_obj: "DbtBouncerManifest"
    models: list["DbtBouncerModel"]
    output_file: Path | None
    output_format: str
    output_only_failures: bool
    run_results: list["DbtBouncerRunResult"]
    seeds: list["DbtBouncerSeed"]
    semantic_models: list["DbtBouncerSemanticModel"]
    show_all_failures: bool
    snapshots: list["DbtBouncerSnapshot"]
    sources: list["DbtBouncerSource"]
    tests: list["DbtBouncerTest"]
    unit_tests: list["UnitTests"]


_CONTEXT_REBUILT: bool = False


def _rebuild_bouncer_context() -> None:
    """Rebuild BouncerContext to resolve forward references after heavy imports."""
    global _CONTEXT_REBUILT
    if _CONTEXT_REBUILT:
        return

    # These imports are required for model_rebuild() to resolve forward references.
    # They trigger heavy module loads (manifest_latest, manifest_v11, etc.)
    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (  # noqa: F401
        UnitTests,
    )
    from dbt_bouncer.artifact_parsers.parsers_catalog import (  # noqa: F401
        DbtBouncerCatalogNode,
    )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (  # noqa: F401
        DbtBouncerExposureBase,
        DbtBouncerMacroBase,
        DbtBouncerManifest,
        DbtBouncerModel,
        DbtBouncerSeed,
        DbtBouncerSemanticModel,
        DbtBouncerSnapshot,
        DbtBouncerSource,
        DbtBouncerTest,
    )
    from dbt_bouncer.artifact_parsers.parsers_run_results import (  # noqa: F401
        DbtBouncerRunResult,
    )
    from dbt_bouncer.config_file_parser import DbtBouncerConfBase  # noqa: F401

    BouncerContext.model_rebuild()
    _CONTEXT_REBUILT = True
