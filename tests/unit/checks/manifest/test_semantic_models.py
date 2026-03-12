import pytest

from dbt_bouncer.testing import check_fails, check_passes


@pytest.mark.parametrize(
    ("ctx_models", "semantic_model_overrides", "check_fn"),
    [
        pytest.param(
            [
                {
                    "access": "public",
                    "unique_id": "model.package_name.model_1",
                },
            ],
            {
                "depends_on": {"nodes": ["model.package_name.model_1"]},
                "fqn": ["package_name", "marts", "finance", "semantic_model_1"],
                "name": "semantic_model_1",
                "original_file_path": "models/marts/finance/_semantic_models.yml",
                "path": "marts/finance/_semantic_models.yml",
                "unique_id": "exposure.package_name.marts.finance.exposure_1",
            },
            check_passes,
            id="public_model",
        ),
        pytest.param(
            [
                {
                    "access": "protected",
                    "unique_id": "model.package_name.model_1",
                },
            ],
            {
                "depends_on": {"nodes": ["model.package_name.model_1"]},
                "fqn": ["package_name", "marts", "finance", "semantic_model_1"],
                "name": "semantic_model_1",
                "original_file_path": "models/marts/finance/_semantic_models.yml",
                "path": "marts/finance/_semantic_models.yml",
                "unique_id": "exposure.package_name.marts.finance.exposure_1",
            },
            check_fails,
            id="protected_model",
        ),
    ],
)
def test_check_semantic_model_based_on_non_public_models(
    ctx_models, semantic_model_overrides, check_fn
):
    check_fn(
        "check_semantic_model_based_on_non_public_models",
        semantic_model=semantic_model_overrides,
        ctx_models=ctx_models,
    )
