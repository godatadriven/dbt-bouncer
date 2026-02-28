"""Tests for the ResourceWrapper protocol."""

import pytest

from dbt_bouncer.artifact_parsers.parsers_catalog import DbtBouncerCatalogNode
from dbt_bouncer.artifact_parsers.parsers_manifest import (
    DbtBouncerExposure,
    DbtBouncerModel,
    DbtBouncerSeed,
    DbtBouncerSemanticModel,
    DbtBouncerSnapshot,
    DbtBouncerSource,
    DbtBouncerTest,
)
from dbt_bouncer.artifact_parsers.parsers_run_results import DbtBouncerRunResult
from dbt_bouncer.resource_adapter import ResourceWrapper


@pytest.mark.parametrize(
    "cls",
    [
        DbtBouncerCatalogNode,
        DbtBouncerExposure,
        DbtBouncerModel,
        DbtBouncerRunResult,
        DbtBouncerSeed,
        DbtBouncerSemanticModel,
        DbtBouncerSnapshot,
        DbtBouncerSource,
        DbtBouncerTest,
    ],
)
def test_wrapper_classes_satisfy_resource_wrapper_protocol(cls: type) -> None:
    """All dbt resource wrapper classes must satisfy the ResourceWrapper protocol."""
    assert issubclass(cls, ResourceWrapper), (
        f"{cls.__name__} does not satisfy ResourceWrapper: "
        "missing 'unique_id' or 'original_file_path'"
    )
