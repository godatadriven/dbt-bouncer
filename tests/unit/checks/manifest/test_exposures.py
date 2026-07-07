import pytest

from dbt_bouncer.testing import check_fails, check_passes

_EXPOSURE_BASE = {
    "depends_on": {"nodes": ["model.package_name.model_1"]},
    "description": "A valid description.",
    "meta": {"maturity": "high", "owner": "Finance"},
    "name": "exposure_1",
    "owner": {"email": "owner@example.com", "name": "Owner Name"},
    "unique_id": "exposure.package_name.exposure_1",
}


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


@pytest.mark.parametrize(
    ("exposure_overrides", "min_description_length", "check_fn"),
    [
        pytest.param(
            {**_EXPOSURE_BASE, "description": "A valid description."},
            None,
            check_passes,
            id="has_description",
        ),
        pytest.param(
            {
                **_EXPOSURE_BASE,
                "description": "A valid description that is long enough.",
            },
            25,
            check_passes,
            id="has_description_min_length",
        ),
        pytest.param(
            {**_EXPOSURE_BASE, "description": ""},
            None,
            check_fails,
            id="empty_description",
        ),
        pytest.param(
            {**_EXPOSURE_BASE, "description": "abc"},
            None,
            check_fails,
            id="too_short_description",
        ),
        pytest.param(
            {**_EXPOSURE_BASE, "description": "Short desc."},
            25,
            check_fails,
            id="below_min_description_length",
        ),
    ],
)
def test_check_exposure_description_populated(
    exposure_overrides, min_description_length, check_fn
):
    kwargs = {"exposure": exposure_overrides}
    if min_description_length is not None:
        kwargs["min_description_length"] = min_description_length
    check_fn("check_exposure_description_populated", **kwargs)


@pytest.mark.parametrize(
    ("keys", "exposure_overrides", "check_fn"),
    [
        pytest.param(
            ["owner"],
            {**_EXPOSURE_BASE, "meta": {"owner": "Finance"}},
            check_passes,
            id="has_key",
        ),
        pytest.param(
            ["maturity", "owner"],
            {**_EXPOSURE_BASE, "meta": {"maturity": "high", "owner": "Finance"}},
            check_passes,
            id="has_multiple_keys",
        ),
        pytest.param(
            ["owner"],
            {**_EXPOSURE_BASE, "meta": {}},
            check_fails,
            id="missing_key",
        ),
    ],
)
def test_check_exposure_has_meta_keys(keys, exposure_overrides, check_fn):
    check_fn("check_exposure_has_meta_keys", keys=keys, exposure=exposure_overrides)


@pytest.mark.parametrize(
    ("exposure_overrides", "required_fields", "check_fn"),
    [
        pytest.param(
            {
                **_EXPOSURE_BASE,
                "owner": {"email": "owner@example.com", "name": "Owner Name"},
            },
            ["email"],
            check_passes,
            id="has_email",
        ),
        pytest.param(
            {
                **_EXPOSURE_BASE,
                "owner": {"email": "owner@example.com", "name": "Owner Name"},
            },
            ["email", "name"],
            check_passes,
            id="has_email_and_name",
        ),
        pytest.param(
            {**_EXPOSURE_BASE, "owner": None},
            ["email"],
            check_fails,
            id="owner_key_absent",
        ),
        pytest.param(
            {**_EXPOSURE_BASE, "owner": {}},
            ["email"],
            check_fails,
            id="no_owner_at_all",
        ),
        pytest.param(
            {**_EXPOSURE_BASE, "owner": {"name": "Owner Name"}},
            ["email"],
            check_fails,
            id="missing_email",
        ),
        pytest.param(
            {**_EXPOSURE_BASE, "owner": {"email": "owner@example.com"}},
            ["email", "name"],
            check_fails,
            id="missing_name",
        ),
    ],
)
def test_check_exposure_has_owner(exposure_overrides, required_fields, check_fn):
    check_fn(
        "check_exposure_has_owner",
        exposure=exposure_overrides,
        required_fields=required_fields,
    )
