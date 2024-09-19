import warnings
from contextlib import nullcontext as does_not_raise

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import (
        SemanticModels,
    )

from dbt_bouncer.checks.manifest.check_semantic_models import (
    CheckSemanticModelOnNonPublicModels,
)
from dbt_bouncer.parsers import (  # noqa: F401
    DbtBouncerModelBase,
    DbtBouncerNodesModel,
    DbtBouncerSemanticModelBase,
)

CheckSemanticModelOnNonPublicModels.model_rebuild()


@pytest.mark.parametrize(
    ("models", "semantic_model", "expectation"),
    [
        (
            [
                DbtBouncerNodesModel(
                    **{
                        "access": "public",
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    },
                )
            ],
            SemanticModels(
                **{
                    "depends_on": {"nodes": ["model.package_name.model_1"]},
                    "fqn": ["package_name", "marts", "finance", "semantic_model_1"],
                    "model": "ref('model_1')",
                    "name": "semantic_model_1",
                    "original_file_path": "models/marts/finance/_semantic_models.yml",
                    "package_name": "package_name",
                    "path": "marts/finance/_semantic_models.yml",
                    "resource_type": "semantic_model",
                    "unique_id": "exposure.package_name.marts.finance.exposure_1",
                }
            ),
            does_not_raise(),
        ),
        (
            [
                DbtBouncerNodesModel(
                    **{
                        "access": "protected",
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    },
                ),
            ],
            SemanticModels(
                **{
                    "depends_on": {"nodes": ["model.package_name.model_1"]},
                    "fqn": ["package_name", "marts", "finance", "semantic_model_1"],
                    "model": "ref('model_1')",
                    "name": "semantic_model_1",
                    "original_file_path": "models/marts/finance/_semantic_models.yml",
                    "package_name": "package_name",
                    "path": "marts/finance/_semantic_models.yml",
                    "resource_type": "semantic_model",
                    "unique_id": "exposure.package_name.marts.finance.exposure_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_semantic_model_based_on_non_public_models(
    models, semantic_model, expectation
):
    with expectation:
        CheckSemanticModelOnNonPublicModels(
            models=models,
            name="check_semantic_model_based_on_non_public_models",
            semantic_model=semantic_model,
        ).execute()
