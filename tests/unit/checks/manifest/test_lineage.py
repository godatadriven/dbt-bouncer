import pytest

from dbt_bouncer.testing import check_fails, check_passes


@pytest.mark.parametrize(
    ("model_overrides", "ctx_models", "upstream_path_pattern", "check_fn"),
    [
        pytest.param(
            {
                "alias": "int_model",
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.stg_model_1"],
                },
                "fqn": ["dbt_bouncer_test_project", "int_model"],
                "name": "int_model",
                "original_file_path": "intermediate/int_model.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "intermediate/int_model.sql",
                "unique_id": "model.dbt_bouncer_test_project.int_model",
            },
            [
                {
                    "alias": "stg_model_1",
                    "fqn": ["dbt_bouncer_test_project", "stg_model_1"],
                    "name": "stg_model_1",
                    "original_file_path": "staging/stg_model_1.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "staging/stg_model_1.sql",
                    "unique_id": "model.dbt_bouncer_test_project.stg_model_1",
                },
            ],
            "^staging",
            check_passes,
            id="upstream_matches_pattern",
        ),
        pytest.param(
            {
                "alias": "int_model",
                "depends_on": {"nodes": ["model.some_other_package.stg_model"]},
                "fqn": ["dbt_bouncer_test_project", "int_model"],
                "name": "int_model",
                "original_file_path": "intermediate/int_model.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "intermediate/int_model.sql",
                "unique_id": "model.dbt_bouncer_test_project.int_model",
            },
            [],
            "^staging",
            check_passes,
            id="cross_package_dependency_skipped",
        ),
        pytest.param(
            {
                "alias": "mart_model",
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.mart_other_model"],
                },
                "fqn": ["dbt_bouncer_test_project", "mart_model"],
                "name": "mart_model",
                "original_file_path": "marts/mart_model.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/mart_model.sql",
                "unique_id": "model.dbt_bouncer_test_project.mart_model",
            },
            [
                {
                    "alias": "mart_other_model",
                    "fqn": ["dbt_bouncer_test_project", "mart_other_model"],
                    "name": "mart_other_model",
                    "original_file_path": "marts/mart_other_model.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "marts/mart_other_model.sql",
                    "unique_id": "model.dbt_bouncer_test_project.mart_other_model",
                },
            ],
            "^intermediate",
            check_fails,
            id="upstream_violates_pattern",
        ),
    ],
)
def test_check_lineage_permitted_upstream_models(
    model_overrides,
    ctx_models,
    upstream_path_pattern,
    check_fn,
):
    check_fn(
        "check_lineage_permitted_upstream_models",
        model=model_overrides,
        upstream_path_pattern=upstream_path_pattern,
        ctx_models=ctx_models,
    )


@pytest.mark.parametrize(
    ("model_overrides", "check_fn"),
    [
        pytest.param(
            {
                "alias": "int_model_2",
                "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
                "fqn": ["package_name", "int_model_2"],
                "name": "int_model_2",
                "original_file_path": "intermediate/int_model_2.sql",
                "path": "intermediate/int_model_2.sql",
                "unique_id": "model.package_name.int_model_2",
            },
            check_passes,
            id="no_seed_dependency",
        ),
        pytest.param(
            {
                "alias": "int_model_2",
                "depends_on": {"nodes": ["seed.package_name.seed_1"]},
                "fqn": ["package_name", "int_model_2"],
                "name": "int_model_2",
                "original_file_path": "intermediate/int_model_2.sql",
                "path": "intermediate/int_model_2.sql",
                "unique_id": "model.package_name.int_model_2",
            },
            check_fails,
            id="has_seed_dependency",
        ),
    ],
)
def test_check_lineage_seed_cannot_be_used(model_overrides, check_fn):
    check_fn(
        "check_lineage_seed_cannot_be_used",
        model=model_overrides,
    )


@pytest.mark.parametrize(
    ("model_overrides", "check_fn"),
    [
        pytest.param(
            {
                "alias": "int_model_2",
                "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
                "fqn": ["package_name", "int_model_2"],
                "name": "int_model_2",
                "original_file_path": "intermediate/int_model_2.sql",
                "path": "intermediate/int_model_2.sql",
                "unique_id": "model.package_name.int_model_2",
            },
            check_passes,
            id="no_source_dependency",
        ),
        pytest.param(
            {
                "alias": "int_model_2",
                "depends_on": {"nodes": ["source.package_name.source_1"]},
                "fqn": ["package_name", "int_model_2"],
                "name": "int_model_2",
                "original_file_path": "intermediate/int_model_2.sql",
                "path": "intermediate/int_model_2.sql",
                "unique_id": "model.package_name.int_model_2",
            },
            check_fails,
            id="has_source_dependency",
        ),
    ],
)
def test_check_lineage_source_cannot_be_used(model_overrides, check_fn):
    check_fn(
        "check_lineage_source_cannot_be_used",
        model=model_overrides,
    )
