import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelHasUniqueTest:
    @pytest.mark.parametrize(
        ("accepted_uniqueness_tests", "model", "ctx_tests"),
        [
            pytest.param(
                ["expect_compound_columns_to_be_unique", "unique"],
                {},
                [{}],
                id="has_unique_test",
            ),
            pytest.param(
                ["my_custom_test", "unique"],
                {},
                [{"test_metadata": {"name": "my_custom_test"}}],
                id="has_custom_unique_test",
            ),
        ],
    )
    def test_passes(self, accepted_uniqueness_tests, model, ctx_tests):
        check_passes(
            "check_model_has_unique_test",
            accepted_uniqueness_tests=accepted_uniqueness_tests,
            model=model,
            ctx_tests=ctx_tests,
        )

    @pytest.mark.parametrize(
        ("accepted_uniqueness_tests", "model", "ctx_tests"),
        [
            pytest.param(
                ["unique"],
                {},
                [{"test_metadata": {"name": "expect_compound_columns_to_be_unique"}}],
                id="missing_unique_test_strict",
            ),
            pytest.param(
                ["expect_compound_columns_to_be_unique", "unique"],
                {},
                [],
                id="missing_unique_test",
            ),
        ],
    )
    def test_fails(self, accepted_uniqueness_tests, model, ctx_tests):
        check_fails(
            "check_model_has_unique_test",
            accepted_uniqueness_tests=accepted_uniqueness_tests,
            model=model,
            ctx_tests=ctx_tests,
        )


class TestCheckModelHasUnitTests:
    def test_passes(self):
        check_passes(
            "check_model_has_unit_tests",
            min_number_of_unit_tests=1,
            model={},
            ctx_unit_tests=[{}],
        )

    def test_fails(self):
        check_fails(
            "check_model_has_unit_tests",
            min_number_of_unit_tests=2,
            model={},
            ctx_unit_tests=[{}],
        )


class TestCheckModelTestCoverage:
    @pytest.mark.parametrize(
        ("min_model_test_coverage_pct", "ctx_models", "ctx_tests"),
        [
            pytest.param(
                100,
                [{}],
                [
                    {
                        "alias": "not_null_model_1_unique",
                        "attached_node": "model.package_name.model_1",
                        "checksum": {"name": "none", "checksum": ""},
                        "column_name": "col_1",
                        "depends_on": {
                            "nodes": ["model.package_name.model_1"],
                        },
                        "fqn": [
                            "package_name",
                            "marts",
                            "finance",
                            "not_null_model_1_unique",
                        ],
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
                ],
                id="100_percent_coverage",
            ),
            pytest.param(
                50,
                [
                    {
                        "alias": "model_1",
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "path": "staging/finance/model_1.sql",
                        "unique_id": "model.package_name.model_1",
                    },
                    {
                        "alias": "model_2",
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "model_2.sql",
                        "path": "staging/finance/model_2.sql",
                        "unique_id": "model.package_name.model_2",
                    },
                ],
                [
                    {
                        "alias": "not_null_model_1_unique",
                        "attached_node": "model.package_name.model_1",
                        "depends_on": {
                            "nodes": ["model.package_name.model_1"],
                        },
                        "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
                    },
                ],
                id="50_percent_coverage",
            ),
        ],
    )
    def test_passes(self, min_model_test_coverage_pct, ctx_models, ctx_tests):
        check_passes(
            "check_model_test_coverage",
            min_model_test_coverage_pct=min_model_test_coverage_pct,
            ctx_models=ctx_models,
            ctx_tests=ctx_tests,
        )

    @pytest.mark.parametrize(
        "min_pct",
        [
            pytest.param(-1, id="negative"),
            pytest.param(101, id="over_100"),
        ],
    )
    def test_raises_value_error_for_invalid_pct(self, min_pct):
        from dbt_bouncer.testing import _run_check

        with pytest.raises(ValueError, match="must be between 0 and 100"):
            _run_check(
                "check_model_test_coverage",
                min_model_test_coverage_pct=min_pct,
                ctx_models=[{}],
                ctx_tests=[{}],
            )

    def test_fails_less_than_100_percent_coverage(self):
        check_fails(
            "check_model_test_coverage",
            min_model_test_coverage_pct=100,
            ctx_models=[
                {
                    "alias": "model_1",
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "path": "staging/finance/model_1.sql",
                    "unique_id": "model.package_name.model_1",
                },
                {
                    "alias": "model_2",
                    "fqn": ["package_name", "model_2"],
                    "name": "model_2",
                    "original_file_path": "model_2.sql",
                    "path": "staging/finance/model_2.sql",
                    "unique_id": "model.package_name.model_2",
                },
            ],
            ctx_tests=[
                {
                    "alias": "not_null_model_1_unique",
                    "attached_node": "model.package_name.model_1",
                    "depends_on": {
                        "nodes": ["model.package_name.model_2"],
                    },
                    "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
                },
            ],
        )
