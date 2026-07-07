import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceHasTests:
    @pytest.mark.parametrize(
        ("min_number_of_tests", "ctx_tests"),
        [
            pytest.param(
                1,
                [
                    {
                        "depends_on": {
                            "nodes": ["source.package_name.source_1.table_1"],
                        },
                    },
                ],
                id="one_test_meets_minimum_of_1",
            ),
            pytest.param(
                2,
                [
                    {
                        "depends_on": {
                            "nodes": ["source.package_name.source_1.table_1"],
                        },
                    },
                    {
                        "depends_on": {
                            "nodes": ["source.package_name.source_1.table_1"],
                        },
                    },
                ],
                id="two_tests_meet_minimum_of_2",
            ),
            pytest.param(
                1,
                [
                    {
                        "depends_on": {
                            "nodes": ["source.package_name.source_1.table_1"],
                        },
                    },
                    {
                        "depends_on": {
                            "nodes": ["model.package_name.model_1"],
                        },
                    },
                ],
                id="one_source_test_and_one_unrelated_test",
            ),
        ],
    )
    def test_passes(self, min_number_of_tests, ctx_tests):
        check_passes(
            "check_source_has_tests",
            min_number_of_tests=min_number_of_tests,
            source={},
            ctx_tests=ctx_tests,
        )

    @pytest.mark.parametrize(
        ("min_number_of_tests", "ctx_tests"),
        [
            pytest.param(
                1,
                [],
                id="no_tests",
            ),
            pytest.param(
                2,
                [
                    {
                        "depends_on": {
                            "nodes": ["source.package_name.source_1.table_1"],
                        },
                    },
                ],
                id="one_test_below_minimum_of_2",
            ),
            pytest.param(
                1,
                [
                    {
                        "depends_on": {
                            "nodes": ["model.package_name.model_1"],
                        },
                    },
                ],
                id="only_unrelated_tests",
            ),
        ],
    )
    def test_fails(self, min_number_of_tests, ctx_tests):
        check_fails(
            "check_source_has_tests",
            min_number_of_tests=min_number_of_tests,
            source={},
            ctx_tests=ctx_tests,
        )


class TestCheckSourceHasTestsInvalidParam:
    @pytest.mark.parametrize(
        "min_number_of_tests",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error(self, min_number_of_tests):
        from dbt_bouncer.testing import _run_check

        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_source_has_tests",
                min_number_of_tests=min_number_of_tests,
                source={},
                ctx_tests=[],
            )
