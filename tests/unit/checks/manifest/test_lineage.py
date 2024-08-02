from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.manifest.check_lineage import (
    check_lineage_permitted_upstream_models,
    check_lineage_seed_cannot_be_used,
    check_lineage_source_cannot_be_used,
)


@pytest.mark.parametrize(
    "check_config, manifest_obj, model, models, expectation",
    [
        (
            {
                "upstream_path_pattern": "^staging",
            },
            "manifest_obj",
            {
                "depends_on": {"nodes": ["model.dbt_bouncer_test_project.stg_model_1"]},
                "unique_id": "model.dbt_bouncer_test_project.int_model_2",
            },
            [
                {
                    "path": "staging/stg_model_1.sql",
                    "unique_id": "model.dbt_bouncer_test_project.stg_model_1",
                }
            ],
            does_not_raise(),
        ),
        (
            {
                "upstream_path_pattern": "^staging",
            },
            "manifest_obj",
            {
                "depends_on": {"nodes": ["model.some_other_package.model_1"]},
                "unique_id": "model.dbt_bouncer_test_project.int_model_2",
            },
            [],
            does_not_raise(),
        ),
        (
            {
                "upstream_path_pattern": "^intermediate",
            },
            "manifest_obj",
            {
                "depends_on": {"nodes": ["model.dbt_bouncer_test_project.mart_model_1"]},
                "unique_id": "model.dbt_bouncer_test_project.mart_model_2",
            },
            [
                {
                    "path": "marts/mart_model_1.sql",
                    "unique_id": "model.dbt_bouncer_test_project.mart_model_1",
                }
            ],
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_lineage_permitted_upstream_models(
    check_config, manifest_obj, model, models, expectation
):
    with expectation:
        check_lineage_permitted_upstream_models(
            check_config=check_config,
            manifest_obj=manifest_obj,
            model=model,
            models=models,
            request=None,
        )


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            {
                "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
                "unique_id": "model.package_name.int_model_2",
            },
            does_not_raise(),
        ),
        (
            {
                "depends_on": {"nodes": ["seed.package_name.seed_1"]},
                "unique_id": "model.package_name.int_model_2",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_lineage_seed_cannot_be_used(model, expectation):
    with expectation:
        check_lineage_seed_cannot_be_used(model=model, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            {
                "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
                "unique_id": "model.package_name.int_model_2",
            },
            does_not_raise(),
        ),
        (
            {
                "depends_on": {"nodes": ["source.package_name.source_1"]},
                "unique_id": "model.package_name.int_model_2",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_lineage_source_cannot_be_used(model, expectation):
    with expectation:
        check_lineage_source_cannot_be_used(model=model, request=None)
