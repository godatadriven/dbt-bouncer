import warnings
from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Nodes6,
)

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)

    from dbt_bouncer.artifact_parsers.parsers_manifest import (  # noqa: F401
        DbtBouncerTest,
        DbtBouncerTestBase,
    )
from dbt_bouncer.checks.manifest.check_tests import CheckSingularTestHasMetaKeys
from dbt_bouncer.utils import clean_path_str

CheckSingularTestHasMetaKeys.model_rebuild()


@pytest.mark.parametrize(
    (
        "keys",
        "singular_test",
        "expectation",
    ),
    [
        (
            ["owner"],
            DbtBouncerTest(
                original_file_path=clean_path_str(
                    "models/marts/finance/_finance__models.yml"
                ),
                test=Nodes6(
                    **{
                        "alias": "not_null_model_1_unique",
                        "attached_node": "model.package_name.model_1",
                        "checksum": {"name": "none", "checksum": ""},
                        "depends_on": {
                            "nodes": [
                                "model.package_name.model_1",
                            ],
                        },
                        "fqn": [
                            "package_name",
                            "marts",
                            "finance",
                            "not_null_model_1_unique",
                        ],
                        "meta": {"owner": "Ronny Ronson"},
                        "name": "not_null_model_1_unique",
                        "original_file_path": "models/marts/finance/_finance__models.yml",
                        "package_name": "package_name",
                        "path": "not_null_model_1_unique.sql",
                        "resource_type": "test",
                        "schema": "main",
                        "test_metadata": {
                            "name": "expect_compound_columns_to_be_unique",
                        },
                        "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
                    },
                ),
                unique_id="test.package_name.not_null_model_1_unique.cf6c17daed",
            ),
            does_not_raise(),
        ),
        (
            ["owner"],
            DbtBouncerTest(
                original_file_path=clean_path_str(
                    "models/marts/finance/_finance__models.yml"
                ),
                test=Nodes6(
                    **{
                        "alias": "not_null_model_1_unique",
                        "attached_node": "model.package_name.model_1",
                        "checksum": {"name": "none", "checksum": ""},
                        "depends_on": {
                            "nodes": [
                                "model.package_name.model_1",
                            ],
                        },
                        "fqn": [
                            "package_name",
                            "marts",
                            "finance",
                            "not_null_model_1_unique",
                        ],
                        "meta": {},
                        "name": "not_null_model_1_unique",
                        "original_file_path": "models/marts/finance/_finance__models.yml",
                        "package_name": "package_name",
                        "path": "not_null_model_1_unique.sql",
                        "resource_type": "test",
                        "schema": "main",
                        "test_metadata": {
                            "name": "expect_compound_columns_to_be_unique",
                        },
                        "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
                    },
                ),
                unique_id="test.package_name.not_null_model_1_unique.cf6c17daed",
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_singular_test_has_meta_keys(
    keys,
    singular_test,
    expectation,
):
    with expectation:
        CheckSingularTestHasMetaKeys(
            keys=keys,
            name="check_singular_test_has_meta_keys",
            singular_test=singular_test,
        ).execute()
