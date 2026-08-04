import logging

import pytest

from dbt_bouncer.testing import _run_check, check_fails, check_passes


def _model(name: str) -> dict:
    """Build a model override dict keyed off the model name.

    Returns:
        dict: A manifest model node dict.

    """
    return {
        "alias": name,
        "fqn": ["package_name", name],
        "name": name,
        "original_file_path": f"{name}.sql",
        "path": f"staging/finance/{name}.sql",
        "unique_id": f"model.package_name.{name}",
    }


def _test_on(model_name: str, *, depends_on: str | None = None) -> dict:
    """Build a test node attached to `model_name`.

    Args:
        model_name: The model the test is attached to.
        depends_on: The model the test depends on, defaulting to `model_name`.

    Returns:
        dict: A manifest test node dict.

    """
    target = depends_on if depends_on is not None else model_name
    return {
        "alias": f"not_null_{model_name}",
        "attached_node": f"model.package_name.{model_name}",
        "depends_on": {"nodes": [f"model.package_name.{target}"]},
        "unique_id": f"test.package_name.not_null_{model_name}.cf6c17daed",
    }


_DBT_18_MANIFEST_METADATA = {"metadata": {"dbt_version": "1.8.0"}}


class TestCheckModelHasTestsByName:
    @pytest.mark.parametrize(
        ("test_names", "min_number_of_tests", "ctx_tests", "check_fn"),
        [
            pytest.param(
                ["not_null", "unique"],
                1,
                [{"test_metadata": {"name": "not_null"}}],
                check_passes,
                id="one_matching_schema_test",
            ),
            pytest.param(
                ["not_null", "unique"],
                2,
                [
                    {"test_metadata": {"name": "not_null"}},
                    {"test_metadata": {"name": "unique"}},
                ],
                check_passes,
                id="two_matching_schema_tests",
            ),
            pytest.param(
                ["my_singular_test"],
                1,
                [{"test_metadata": None, "name": "my_singular_test"}],
                check_passes,
                id="matching_singular_test",
            ),
            # A falsy test_metadata falls back to the test's own `name`, which is
            # how singular tests are matched.
            pytest.param(
                ["my_singular_test"],
                1,
                [{"test_metadata": {}, "name": "my_singular_test"}],
                check_passes,
                id="empty_test_metadata_falls_back_to_test_name",
            ),
            pytest.param(
                ["not_null"],
                1,
                [],
                check_fails,
                id="no_tests",
            ),
            pytest.param(
                ["not_null"],
                1,
                [{"test_metadata": {"name": "unique"}}],
                check_fails,
                id="test_name_does_not_match",
            ),
            pytest.param(
                ["not_null", "unique"],
                2,
                [{"test_metadata": {"name": "not_null"}}],
                check_fails,
                id="one_test_below_minimum_of_2",
            ),
            pytest.param(
                [],
                1,
                [{"test_metadata": {"name": "not_null"}}],
                check_fails,
                id="no_test_names_matches_nothing",
            ),
        ],
    )
    def test_check_model_has_tests_by_name(
        self, test_names, min_number_of_tests, ctx_tests, check_fn
    ):
        check_fn(
            "check_model_has_tests_by_name",
            test_names=test_names,
            min_number_of_tests=min_number_of_tests,
            model={},
            ctx_tests=ctx_tests,
        )

    def test_raises_value_error_for_zero_minimum(self):
        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_has_tests_by_name",
                test_names=["not_null"],
                min_number_of_tests=0,
                model={},
                ctx_tests=[],
            )

    def test_failure_message_reports_the_count_and_minimum(self):
        check_fails(
            "check_model_has_tests_by_name",
            test_names=["not_null"],
            min_number_of_tests=2,
            model={},
            ctx_tests=[{"test_metadata": {"name": "not_null"}}],
            match=(
                r"has 1 test\(s\) matching \['not_null'\], fewer than the minimum 2\."
            ),
        )


class TestCheckModelHasTestsByType:
    @pytest.mark.parametrize(
        (
            "min_number_of_schema_tests",
            "min_number_of_data_tests",
            "ctx_tests",
            "check_fn",
        ),
        [
            pytest.param(
                1,
                0,
                [{"test_metadata": {"name": "not_null"}}],
                check_passes,
                id="one_schema_test_meets_minimum",
            ),
            pytest.param(
                0,
                1,
                [{"test_metadata": None, "name": "my_data_test"}],
                check_passes,
                id="one_data_test_meets_minimum",
            ),
            pytest.param(
                1,
                1,
                [
                    {"test_metadata": {"name": "not_null"}},
                    {"test_metadata": None, "name": "my_data_test"},
                ],
                check_passes,
                id="one_schema_and_one_data_test",
            ),
            pytest.param(
                1,
                0,
                [],
                check_fails,
                id="no_tests_schema_minimum_not_met",
            ),
            pytest.param(
                0,
                1,
                [],
                check_fails,
                id="no_tests_data_minimum_not_met",
            ),
            pytest.param(
                1,
                1,
                [{"test_metadata": {"name": "not_null"}}],
                check_fails,
                id="schema_ok_but_data_minimum_not_met",
            ),
            pytest.param(
                2,
                0,
                [{"test_metadata": {"name": "not_null"}}],
                check_fails,
                id="one_schema_test_below_minimum_of_2",
            ),
            pytest.param(
                1,
                1,
                [],
                check_fails,
                id="both_minimums_not_met",
            ),
        ],
    )
    def test_check_model_has_tests_by_type(
        self,
        min_number_of_schema_tests,
        min_number_of_data_tests,
        ctx_tests,
        check_fn,
    ):
        check_fn(
            "check_model_has_tests_by_type",
            min_number_of_schema_tests=min_number_of_schema_tests,
            min_number_of_data_tests=min_number_of_data_tests,
            model={},
            ctx_tests=ctx_tests,
        )

    def test_failure_message_joins_both_shortfalls(self):
        # Both minimums unmet, so the two clauses are joined with "; and ".
        check_fails(
            "check_model_has_tests_by_type",
            min_number_of_schema_tests=1,
            min_number_of_data_tests=1,
            model={},
            ctx_tests=[],
            match=(
                r"has 0 schema test\(s\), fewer than the minimum 1"
                r"; and 0 data test\(s\), fewer than the minimum 1\."
            ),
        )

    def test_raises_value_error_for_both_zero(self):
        with pytest.raises(ValueError, match="At least one of"):
            _run_check(
                "check_model_has_tests_by_type",
                min_number_of_data_tests=0,
                min_number_of_schema_tests=0,
                model={},
                ctx_tests=[],
            )

    def test_raises_value_error_for_negative_minimum(self):
        with pytest.raises(ValueError, match="greater than or equal to 0"):
            _run_check(
                "check_model_has_tests_by_type",
                min_number_of_schema_tests=-1,
                min_number_of_data_tests=1,
                model={},
                ctx_tests=[],
            )


class TestCheckModelHasUniqueTest:
    @pytest.mark.parametrize(
        ("accepted_uniqueness_tests", "ctx_tests", "check_fn"),
        [
            pytest.param(
                ["expect_compound_columns_to_be_unique", "unique"],
                [{}],
                check_passes,
                id="has_unique_test",
            ),
            pytest.param(
                ["my_custom_test", "unique"],
                [{"test_metadata": {"name": "my_custom_test"}}],
                check_passes,
                id="has_custom_unique_test",
            ),
            pytest.param(
                ["unique"],
                [{"test_metadata": {"name": "expect_compound_columns_to_be_unique"}}],
                check_fails,
                id="missing_unique_test_strict",
            ),
            pytest.param(
                ["expect_compound_columns_to_be_unique", "unique"],
                [],
                check_fails,
                id="missing_unique_test",
            ),
            # An accepted list of None accepts nothing.
            pytest.param(None, [{}], check_fails, id="accepted_list_is_none"),
            pytest.param([], [{}], check_fails, id="accepted_list_is_empty"),
            # A namespaced test is matched as "<namespace>.<name>", so the same
            # bare name under a different package is not accepted.
            pytest.param(
                ["unique"],
                [{"test_metadata": {"name": "unique", "namespace": "other_pkg"}}],
                check_fails,
                id="namespaced_test_not_matched_by_bare_name",
            ),
        ],
    )
    def test_check_model_has_unique_test(
        self, accepted_uniqueness_tests, ctx_tests, check_fn
    ):
        check_fn(
            "check_model_has_unique_test",
            accepted_uniqueness_tests=accepted_uniqueness_tests,
            model={},
            ctx_tests=ctx_tests,
        )

    @pytest.mark.parametrize(
        "ctx_tests",
        [
            pytest.param([{}], id="bare_unique_test"),
            pytest.param(
                [
                    {
                        "test_metadata": {
                            "name": "unique_combination_of_columns",
                            "namespace": "dbt_utils",
                        }
                    }
                ],
                id="namespaced_dbt_utils_test",
            ),
            pytest.param(
                [
                    {
                        "test_metadata": {
                            "name": "expect_compound_columns_to_be_unique",
                            "namespace": "dbt_expectations",
                        }
                    }
                ],
                id="namespaced_dbt_expectations_test",
            ),
        ],
    )
    def test_default_accepted_uniqueness_tests(self, ctx_tests):
        # Exercises the documented default list, including its namespaced
        # entries, without passing accepted_uniqueness_tests explicitly.
        check_passes("check_model_has_unique_test", model={}, ctx_tests=ctx_tests)


class TestCheckModelHasUnitTests:
    @pytest.mark.parametrize(
        ("min_number_of_unit_tests", "ctx_unit_tests", "check_fn"),
        [
            pytest.param(1, [{}], check_passes, id="has_unit_test"),
            pytest.param(2, [{}], check_fails, id="not_enough_unit_tests"),
            pytest.param(1, [], check_fails, id="no_unit_tests"),
        ],
    )
    def test_check_model_has_unit_tests(
        self, min_number_of_unit_tests, ctx_unit_tests, check_fn
    ):
        check_fn(
            "check_model_has_unit_tests",
            min_number_of_unit_tests=min_number_of_unit_tests,
            model={},
            ctx_unit_tests=ctx_unit_tests,
            ctx_manifest_obj=_DBT_18_MANIFEST_METADATA,
        )

    def test_dbt_version_below_1_8_0_warns_instead_of_failing(self, caplog):
        with caplog.at_level(logging.WARNING):
            check_passes(
                "check_model_has_unit_tests",
                min_number_of_unit_tests=5,
                model={},
                ctx_manifest_obj={"metadata": {"dbt_version": "1.7.0"}},
            )
        assert "1.8.0" in caplog.text

    def test_failure_message_reports_the_count_and_minimum(self):
        check_fails(
            "check_model_has_unit_tests",
            min_number_of_unit_tests=2,
            model={},
            ctx_unit_tests=[{}],
            ctx_manifest_obj=_DBT_18_MANIFEST_METADATA,
            match=r"has 1 unit tests, this is less than the minimum of 2\.",
        )


class TestCheckModelTestCoverage:
    @pytest.mark.parametrize(
        ("min_model_test_coverage_pct", "ctx_models", "ctx_tests", "check_fn"),
        [
            pytest.param(
                100,
                [{}],
                [_test_on("model_1")],
                check_passes,
                id="100_percent_coverage",
            ),
            pytest.param(
                50,
                [_model("model_1"), _model("model_2")],
                [_test_on("model_1")],
                check_passes,
                id="50_percent_coverage",
            ),
            pytest.param(
                0,
                [_model("model_1")],
                [],
                check_passes,
                id="zero_percent_minimum_always_passes",
            ),
            pytest.param(
                100,
                [_model("model_1"), _model("model_2")],
                # The test depends on model_2, so model_1 is uncovered.
                [_test_on("model_1", depends_on="model_2")],
                check_fails,
                id="less_than_100_percent_coverage",
            ),
            pytest.param(
                75,
                [_model("model_1"), _model("model_2")],
                [_test_on("model_1")],
                check_fails,
                id="coverage_below_minimum",
            ),
        ],
    )
    def test_check_model_test_coverage(
        self, min_model_test_coverage_pct, ctx_models, ctx_tests, check_fn
    ):
        check_fn(
            "check_model_test_coverage",
            min_model_test_coverage_pct=min_model_test_coverage_pct,
            ctx_models=ctx_models,
            ctx_tests=ctx_tests,
        )

    def test_test_without_depends_on_is_ignored(self):
        check_passes(
            "check_model_test_coverage",
            min_model_test_coverage_pct=0,
            ctx_models=[_model("model_1")],
            ctx_tests=[{"depends_on": None}],
        )

    @pytest.mark.parametrize(
        "min_pct",
        [
            pytest.param(0, id="zero_percent_minimum"),
            pytest.param(100, id="hundred_percent_minimum"),
        ],
    )
    def test_empty_model_list_passes(self, min_pct):
        # With no models there is nothing left untested, so coverage is
        # vacuously 100% and the check passes rather than dividing by zero.
        check_passes(
            "check_model_test_coverage",
            min_model_test_coverage_pct=min_pct,
            ctx_models=[],
            ctx_tests=[],
        )

    @pytest.mark.parametrize(
        ("min_pct", "match_pattern"),
        [
            pytest.param(-1, "greater than or equal to 0", id="negative"),
            pytest.param(101, "less than or equal to 100", id="over_100"),
        ],
    )
    def test_raises_value_error_for_invalid_pct(self, min_pct, match_pattern):
        with pytest.raises(ValueError, match=match_pattern):
            _run_check(
                "check_model_test_coverage",
                min_model_test_coverage_pct=min_pct,
                ctx_models=[{}],
                ctx_tests=[{}],
            )

    def test_failure_message_reports_the_coverage_and_minimum(self):
        check_fails(
            "check_model_test_coverage",
            min_model_test_coverage_pct=100,
            ctx_models=[_model("model_1"), _model("model_2")],
            ctx_tests=[_test_on("model_1")],
            match=(
                r"Only 50\.0% of models have at least one test, this is less than "
                r"the permitted minimum of 100\.0%\."
            ),
        )
