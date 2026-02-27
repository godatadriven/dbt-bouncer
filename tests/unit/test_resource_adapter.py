"""Tests for the ResourceWrapper protocol."""

import pytest


@pytest.mark.parametrize(
    "cls_path",
    [
        "dbt_bouncer.artifact_parsers.parsers_catalog:DbtBouncerCatalogNode",
        "dbt_bouncer.artifact_parsers.parsers_manifest:DbtBouncerExposure",
        "dbt_bouncer.artifact_parsers.parsers_manifest:DbtBouncerModel",
        "dbt_bouncer.artifact_parsers.parsers_manifest:DbtBouncerSeed",
        "dbt_bouncer.artifact_parsers.parsers_manifest:DbtBouncerSemanticModel",
        "dbt_bouncer.artifact_parsers.parsers_manifest:DbtBouncerSnapshot",
        "dbt_bouncer.artifact_parsers.parsers_manifest:DbtBouncerSource",
        "dbt_bouncer.artifact_parsers.parsers_manifest:DbtBouncerTest",
        "dbt_bouncer.artifact_parsers.parsers_run_results:DbtBouncerRunResult",
    ],
)
def test_wrapper_classes_satisfy_resource_wrapper_protocol(cls_path: str) -> None:
    """All dbt resource wrapper classes must satisfy the ResourceWrapper protocol."""
    import importlib

    module_path, cls_name = cls_path.split(":")
    module = importlib.import_module(module_path)
    cls = getattr(module, cls_name)

    # Verify the class has the required attributes declared in its annotations
    annotations = {}
    for klass in reversed(cls.__mro__):
        annotations.update(getattr(klass, "__annotations__", {}))

    assert "unique_id" in annotations, f"{cls_name} is missing 'unique_id' annotation"
    assert "original_file_path" in annotations, (
        f"{cls_name} is missing 'original_file_path' annotation"
    )
