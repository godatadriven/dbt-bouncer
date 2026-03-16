"""Tests for the dbt_bouncer.testing module."""

from __future__ import annotations

import pytest

from dbt_bouncer.testing import (
    _build_check_context,
    _build_manifest_obj,
    _build_resource,
    _build_resource_list,
    check_fails,
    check_passes,
)


class TestBuildResource:
    def test_returns_wrapped_dict(self):
        resource = _build_resource("model", {})
        assert resource.name == "model_1"
        assert resource.unique_id == "model.package_name.model_1"

    def test_overrides_are_applied(self):
        resource = _build_resource("model", {"name": "custom_model"})
        assert resource.name == "custom_model"

    def test_catalog_node_defaults(self):
        resource = _build_resource("catalog_node", {})
        assert resource.metadata.name == "table_1"

    def test_run_result_defaults(self):
        resource = _build_resource("run_result", {})
        assert resource.execution_time == 1


class TestBuildResourceList:
    def test_returns_list_of_wrapped_dicts(self):
        resources = _build_resource_list("model", [{"name": "m1"}, {"name": "m2"}])
        assert len(resources) == 2
        assert resources[0].name == "m1"
        assert resources[1].name == "m2"

    def test_empty_list(self):
        resources = _build_resource_list("model", [])
        assert resources == []


class TestBuildManifestObj:
    def test_default_manifest(self):
        manifest = _build_manifest_obj()
        assert manifest.manifest.metadata.dbt_version == "1.11.0a1"

    def test_override_manifest(self):
        manifest = _build_manifest_obj(
            {"metadata": {"dbt_version": "2.0.0", "adapter_type": "duckdb"}}
        )
        assert manifest.manifest.metadata.dbt_version == "2.0.0"


class TestBuildCheckContext:
    def test_empty_context(self):
        ctx = _build_check_context()
        assert ctx.manifest_obj is not None
        assert ctx.models == []

    def test_ctx_models(self):
        ctx = _build_check_context(ctx_models=[{"name": "m1"}])
        assert len(ctx.models) == 1
        assert ctx.models[0].name == "m1"

    def test_ctx_manifest_obj_dict(self):
        ctx = _build_check_context(
            ctx_manifest_obj={"metadata": {"dbt_version": "2.0.0", "adapter_type": "x"}}
        )
        assert ctx.manifest_obj.manifest.metadata.dbt_version == "2.0.0"


class TestCheckPasses:
    def test_check_passes_with_valid_model(self):
        check_passes(
            "check_model_description_populated",
            model={"description": "A good description that is long enough."},
        )

    def test_check_passes_fails_on_invalid(self):
        with pytest.raises(pytest.fail.Exception):
            check_passes(
                "check_model_description_populated",
                model={"description": ""},
            )


class TestCheckFails:
    def test_check_fails_with_invalid_model(self):
        check_fails(
            "check_model_description_populated",
            model={"description": ""},
        )

    def test_check_fails_raises_on_valid(self):
        with pytest.raises(pytest.fail.Exception):
            check_fails(
                "check_model_description_populated",
                model={"description": "A good description that is long enough."},
            )


class TestCheckWithParams:
    def test_check_with_params_passes(self):
        check_passes(
            "check_model_names",
            model={"name": "stg_orders"},
            model_name_pattern="^stg_",
        )

    def test_check_with_params_fails(self):
        check_fails(
            "check_model_names",
            model={"name": "fct_orders"},
            model_name_pattern="^stg_",
        )


class TestCheckWithContext:
    def test_context_check_passes(self):
        check_passes(
            "check_model_documentation_coverage",
            min_model_documentation_coverage_pct=100,
            ctx_models=[
                {
                    "description": "Model 1 description",
                    "name": "model_1",
                    "unique_id": "model.package_name.model_1",
                },
            ],
        )

    def test_context_check_fails(self):
        check_fails(
            "check_model_documentation_coverage",
            min_model_documentation_coverage_pct=100,
            ctx_models=[
                {
                    "description": "",
                    "name": "model_1",
                    "unique_id": "model.package_name.model_1",
                },
            ],
        )


class TestUnknownCheck:
    def test_raises_key_error(self):
        with pytest.raises(KeyError, match="Unknown check name"):
            check_passes("nonexistent_check_name")
