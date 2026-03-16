import pytest

from dbt_bouncer.testing import check_fails, check_passes


@pytest.mark.parametrize(
    (
        "exposure_overrides",
        "maximum_number_of_models",
        "minimum_number_of_models",
        "check_fn",
    ),
    [
        pytest.param(
            {"depends_on": {"nodes": ["model.package_name.model_1"]}},
            100,
            1,
            check_passes,
            id="within_bounds",
        ),
        pytest.param(
            {
                "depends_on": {
                    "nodes": [
                        "model.package_name.model_1",
                        "model.package_name.model_2",
                    ],
                },
            },
            1,
            1,
            check_fails,
            id="exceeds_maximum",
        ),
        pytest.param(
            {"depends_on": {"nodes": ["model.package_name.model_1"]}},
            100,
            2,
            check_fails,
            id="below_minimum",
        ),
    ],
)
def test_check_exposure_based_on_model(
    exposure_overrides, maximum_number_of_models, minimum_number_of_models, check_fn
):
    check_fn(
        "check_exposure_based_on_model",
        exposure=exposure_overrides,
        maximum_number_of_models=maximum_number_of_models,
        minimum_number_of_models=minimum_number_of_models,
    )


@pytest.mark.parametrize(
    ("exposure_overrides", "ctx_models", "check_fn"),
    [
        pytest.param(
            {},
            [
                {
                    "access": "public",
                    "unique_id": "model.package_name.model_1",
                },
            ],
            check_passes,
            id="all_public",
        ),
        pytest.param(
            {},
            [
                {
                    "access": "protected",
                    "unique_id": "model.package_name.model_1",
                },
            ],
            check_fails,
            id="non_public",
        ),
    ],
)
def test_check_exposure_based_on_non_public_models(
    exposure_overrides, ctx_models, check_fn
):
    check_fn(
        "check_exposure_based_on_non_public_models",
        exposure=exposure_overrides,
        ctx_models=ctx_models,
    )


@pytest.mark.parametrize(
    ("exposure_overrides", "materializations_to_include", "ctx_models", "check_fn"),
    [
        pytest.param(
            {},
            ["ephemeral", "view"],
            [
                {
                    "access": "protected",
                    "config": {"materialized": "table"},
                    "unique_id": "model.package_name.model_1",
                },
            ],
            check_passes,
            id="no_view_dependency",
        ),
        pytest.param(
            {
                "depends_on": {
                    "nodes": [
                        "model.package_name.model_1",
                        "model.package_name.model_2",
                    ],
                },
            },
            ["ephemeral", "view"],
            [
                {
                    "access": "protected",
                    "alias": "model_1",
                    "config": {"materialized": "view"},
                    "name": "model_1",
                    "unique_id": "model.package_name.model_1",
                },
                {
                    "access": "protected",
                    "alias": "model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "config": {"materialized": "table"},
                    "fqn": ["package_name", "model_2"],
                    "name": "model_2",
                    "original_file_path": "model_2.sql",
                    "package_name": "package_name",
                    "path": "model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_2",
                },
            ],
            check_fails,
            id="view_dependency_multi_model",
        ),
        pytest.param(
            {},
            ["ephemeral", "view"],
            [
                {
                    "access": "protected",
                    "config": {"materialized": "view"},
                    "unique_id": "model.package_name.model_1",
                },
            ],
            check_fails,
            id="view_dependency_single_model",
        ),
    ],
)
def test_check_exposure_based_on_view(
    exposure_overrides,
    materializations_to_include,
    ctx_models,
    check_fn,
):
    check_fn(
        "check_exposure_based_on_view",
        exposure=exposure_overrides,
        materializations_to_include=materializations_to_include,
        ctx_models=ctx_models,
    )
