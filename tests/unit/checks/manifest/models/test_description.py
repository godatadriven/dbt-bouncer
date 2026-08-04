import re

import pytest

from dbt_bouncer.testing import _run_check, check_fails, check_passes


def _model_with_columns(name: str, columns: dict | None) -> dict:
    """Build a model override carrying the given `columns`.

    Returns:
        dict: A manifest model node dict.

    """
    return {
        "columns": columns,
        "name": name,
        "unique_id": f"model.package_name.{name}",
    }


def _col(name: str, description: str | None = None) -> dict:
    """Build a column dict, omitting `description` entirely when None.

    Returns:
        dict: A column dict.

    """
    col = {"name": name}
    if description is not None:
        col["description"] = description
    return col


class TestCheckColumnDescriptionsAreConsistent:
    @pytest.mark.parametrize(
        ("models_list", "check_fn"),
        [
            pytest.param(
                [
                    _model_with_columns("model_1", {"id": _col("id", "Primary key.")}),
                    _model_with_columns("model_2", {"id": _col("id", "Primary key.")}),
                ],
                check_passes,
                id="same_name_same_description",
            ),
            pytest.param(
                [
                    _model_with_columns(
                        "model_1", {"order_id": _col("order_id", "Order PK.")}
                    ),
                    _model_with_columns(
                        "model_2", {"customer_id": _col("customer_id", "Customer FK.")}
                    ),
                ],
                check_passes,
                id="all_distinct_column_names",
            ),
            pytest.param(
                [
                    _model_with_columns(
                        "model_1",
                        {"id": _col("id", ""), "name": _col("name", "   ")},
                    ),
                    _model_with_columns(
                        "model_2", {"id": _col("id", "Real description.")}
                    ),
                ],
                check_passes,
                id="empty_descriptions_ignored",
            ),
            pytest.param(
                [
                    _model_with_columns("model_1", None),
                    _model_with_columns("model_2", {"id": _col("id", "Primary key.")}),
                ],
                check_passes,
                id="none_columns_no_conflict",
            ),
            # Descriptions are stripped before comparison, so incidental
            # whitespace is not a conflict.
            pytest.param(
                [
                    _model_with_columns(
                        "model_1", {"id": _col("id", "  Primary key.  ")}
                    ),
                    _model_with_columns("model_2", {"id": _col("id", "Primary key.")}),
                ],
                check_passes,
                id="descriptions_differing_only_by_whitespace",
            ),
            pytest.param(
                [
                    _model_with_columns("model_1", {"id": _col("id")}),
                    _model_with_columns("model_2", {"id": _col("id", "Primary key.")}),
                ],
                check_passes,
                id="absent_description_ignored",
            ),
            pytest.param([], check_passes, id="no_models_vacuously_passes"),
            pytest.param(
                [
                    _model_with_columns("model_1", {"id": _col("id", "Primary key.")}),
                    _model_with_columns(
                        "model_2", {"id": _col("id", "Surrogate key.")}
                    ),
                ],
                check_fails,
                id="same_name_different_descriptions",
            ),
            pytest.param(
                [
                    _model_with_columns("model_1", {"id": _col("id", "Primary key.")}),
                    _model_with_columns(
                        "model_2", {"id": _col("id", "Surrogate key.")}
                    ),
                    _model_with_columns(
                        "model_3",
                        {"other_col": _col("other_col", "Some other column.")},
                    ),
                ],
                check_fails,
                id="three_models_two_conflict_on_id",
            ),
            pytest.param(
                [
                    _model_with_columns(
                        "model_1", {"id": _col("id", "Description A.")}
                    ),
                    _model_with_columns(
                        "model_2", {"id": _col("id", "Description B.")}
                    ),
                    _model_with_columns(
                        "model_3", {"id": _col("id", "Description C.")}
                    ),
                ],
                check_fails,
                id="three_way_conflict_on_same_column",
            ),
        ],
    )
    def test_check_column_descriptions_are_consistent(self, models_list, check_fn):
        check_fn(
            "check_column_descriptions_are_consistent",
            ctx_models=models_list,
        )

    def test_failure_message_lists_the_conflicting_descriptions_sorted(self):
        check_fails(
            "check_column_descriptions_are_consistent",
            ctx_models=[
                _model_with_columns("model_1", {"id": _col("id", "Surrogate key.")}),
                _model_with_columns("model_2", {"id": _col("id", "Primary key.")}),
            ],
            match=r"'id': \['Primary key\.', 'Surrogate key\.'\]",
        )


class TestCheckModelDescriptionContainsRegexPattern:
    @pytest.mark.parametrize(
        ("model_override", "regexp_pattern", "check_fn"),
        [
            pytest.param(
                {"description": "Description that contains the pattern to match."},
                ".*pattern to match.*",
                check_passes,
                id="contains_pattern_single_line",
            ),
            pytest.param(
                {
                    "description": "A\n                        multiline\n                        description\n                        with the pattern to match.\n                        ",
                },
                ".*pattern to match.*",
                check_passes,
                id="contains_pattern_multiline",
            ),
            pytest.param(
                {
                    "description": "Description with\n                    the\n                    pattern to match.",
                },
                ".*pattern to match.*",
                check_passes,
                id="contains_pattern_multiline_split",
            ),
            # The pattern is compiled with re.DOTALL, so `.` spans newlines.
            pytest.param(
                {"description": "A\nB"},
                "A.B",
                check_passes,
                id="dotall_dot_matches_newline",
            ),
            pytest.param(
                {"description": "abc"},
                "  ^abc$  ",
                check_passes,
                id="padded_pattern_stripped_before_matching",
            ),
            pytest.param(
                {"description": ""},
                ".*pattern to match.*",
                check_fails,
                id="empty_description",
            ),
            pytest.param(
                {"description": " "},
                ".*pattern to match.*",
                check_fails,
                id="whitespace_description",
            ),
            pytest.param(
                {"description": "\n                        "},
                ".*pattern to match.*",
                check_fails,
                id="multiline_whitespace_description",
            ),
            pytest.param(
                {"description": "Description with a pattern that does not match."},
                ".*pattern to match.*",
                check_fails,
                id="does_not_contain_pattern",
            ),
            # `re.match` anchors at the start, so a pattern that only matches
            # mid-description is not enough.
            pytest.param(
                {"description": "prefix then the tail"},
                "the tail",
                check_fails,
                id="pattern_not_anchored_at_start",
            ),
        ],
    )
    def test_check_model_description_contains_regex_pattern(
        self, model_override, regexp_pattern, check_fn
    ):
        check_fn(
            "check_model_description_contains_regex_pattern",
            model=model_override,
            regexp_pattern=regexp_pattern,
        )

    def test_none_description_is_coerced_to_the_string_none(self):
        # The check calls str(model.description) rather than defaulting to "",
        # so a None description is matched as the literal "None".
        check_passes(
            "check_model_description_contains_regex_pattern",
            model={"description": None},
            regexp_pattern="None",
        )

    def test_invalid_regex_raises_re_error(self):
        check_fails(
            "check_model_description_contains_regex_pattern",
            model={"description": "abc"},
            regexp_pattern="(unclosed",
            expected_exception=re.error,
            match="Invalid regex pattern",
        )


class TestCheckModelDescriptionPopulated:
    @pytest.mark.parametrize(
        ("model_override", "check_fn"),
        [
            pytest.param(
                {"description": "Description that is more than 4 characters."},
                check_passes,
                id="populated_description",
            ),
            pytest.param(
                {
                    "description": "A\n                        multiline\n                        description\n                        ",
                },
                check_passes,
                id="multiline_description",
            ),
            pytest.param(
                {"description": "abcd"}, check_passes, id="exactly_min_length"
            ),
            pytest.param({"description": ""}, check_fails, id="empty_description"),
            pytest.param(
                {"description": " "}, check_fails, id="whitespace_description"
            ),
            pytest.param(
                {"description": "\n                        "},
                check_fails,
                id="multiline_whitespace_description",
            ),
            pytest.param({"description": "-"}, check_fails, id="too_short_description"),
            pytest.param({"description": "null"}, check_fails, id="null_description"),
            pytest.param(
                {"description": "abc"}, check_fails, id="one_below_min_length"
            ),
            pytest.param({"description": None}, check_fails, id="description_is_none"),
            pytest.param({}, check_fails, id="description_absent"),
        ],
    )
    def test_check_model_description_populated(self, model_override, check_fn):
        check_fn("check_model_description_populated", model=model_override)

    @pytest.mark.parametrize(
        ("description", "min_description_length", "check_fn"),
        [
            pytest.param(
                "1234567890", 10, check_passes, id="description_at_min_length"
            ),
            pytest.param(
                "123456789", 10, check_fails, id="description_one_below_min_length"
            ),
        ],
    )
    def test_check_model_description_populated_boundary(
        self, description, min_description_length, check_fn
    ):
        check_fn(
            "check_model_description_populated",
            model={"description": description},
            min_description_length=min_description_length,
        )

    def test_min_description_length_must_be_positive(self):
        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_description_populated",
                model={"description": "abcd"},
                min_description_length=0,
            )


class TestCheckModelDocumentationCoverage:
    @pytest.mark.parametrize(
        ("min_pct", "models_list", "check_fn"),
        [
            pytest.param(
                100,
                [
                    {
                        "description": "Model 2 description",
                        "name": "model_2",
                        "unique_id": "model.package_name.model_2",
                    },
                ],
                check_passes,
                id="100_percent_coverage",
            ),
            pytest.param(
                50,
                [
                    {
                        "description": "Model 1 description",
                        "name": "model_1",
                        "unique_id": "model.package_name.model_1",
                    },
                    {
                        "description": "",
                        "name": "model_2",
                        "unique_id": "model.package_name.model_2",
                    },
                ],
                check_passes,
                # The comparison is `<`, so coverage exactly at the minimum passes.
                id="coverage_exactly_at_minimum",
            ),
            pytest.param(
                0,
                [
                    {
                        "description": "",
                        "name": "model_1",
                        "unique_id": "model.package_name.model_1",
                    },
                ],
                check_passes,
                id="zero_percent_minimum_always_passes",
            ),
            pytest.param(
                100,
                [
                    {
                        "description": "",
                        "name": "model_2",
                        "unique_id": "model.package_name.model_2",
                    },
                ],
                check_fails,
                id="0_percent_coverage",
            ),
            pytest.param(
                75,
                [
                    {
                        "description": "Model 1 description",
                        "name": "model_1",
                        "unique_id": "model.package_name.model_1",
                    },
                    {
                        "description": "",
                        "name": "model_2",
                        "unique_id": "model.package_name.model_2",
                    },
                ],
                check_fails,
                id="coverage_one_step_below_minimum",
            ),
        ],
    )
    def test_check_model_documentation_coverage(self, min_pct, models_list, check_fn):
        check_fn(
            "check_model_documentation_coverage",
            min_model_documentation_coverage_pct=min_pct,
            ctx_models=models_list,
        )

    def test_empty_model_list_raises_zero_division_error(self):
        # Documents current behaviour: the coverage percentage is computed as
        # `num_models_with_descriptions / num_models` with no guard for an empty
        # model list, so a project with no models raises rather than passing.
        with pytest.raises(ZeroDivisionError):
            _run_check(
                "check_model_documentation_coverage",
                min_model_documentation_coverage_pct=100,
                ctx_models=[],
            )


class TestCheckModelDocumentationCoverageInvalidParam:
    @pytest.mark.parametrize(
        ("min_pct", "match_pattern"),
        [
            pytest.param(-1, "greater than or equal to 0", id="negative"),
            pytest.param(101, "less than or equal to 100", id="over_100"),
        ],
    )
    def test_raises_value_error(self, min_pct, match_pattern):
        with pytest.raises(ValueError, match=match_pattern):
            _run_check(
                "check_model_documentation_coverage",
                min_model_documentation_coverage_pct=min_pct,
                ctx_models=[{}],
            )


class TestCheckModelDocumentedInSameDirectory:
    @pytest.mark.parametrize(
        ("model_override", "check_fn"),
        [
            pytest.param(
                {
                    "original_file_path": "models/staging/model_1.sql",
                    "patch_path": "package_name://models/staging/_schema.yml",
                    "path": "staging/model_1.sql",
                },
                check_passes,
                id="documented_in_same_directory",
            ),
            # `clean_path_str` normalises backslashes, so a Windows-style
            # patch_path resolves to the same directory.
            pytest.param(
                {
                    "original_file_path": "models/staging/model_1.sql",
                    "patch_path": "package_name:\\\\models\\\\staging\\\\_schema.yml",
                    "path": "staging/model_1.sql",
                },
                check_passes,
                id="windows_style_patch_path_normalised",
            ),
            pytest.param(
                {
                    "original_file_path": "models/staging/finance/model_1.sql",
                    "patch_path": "package_name://models/staging/_schema.yml",
                    "path": "staging/finance/model_1.sql",
                },
                check_fails,
                id="documented_in_parent_directory",
            ),
            pytest.param(
                {
                    "original_file_path": "models/staging/model_1.sql",
                    "patch_path": "package_name://models/marts/_schema.yml",
                    "path": "staging/model_1.sql",
                },
                check_fails,
                id="documented_in_sibling_directory",
            ),
            # `patch_path` is truncated from the first "models" segment onwards;
            # a patch_path without one is compared in full.
            pytest.param(
                {
                    "original_file_path": "models/staging/model_1.sql",
                    "patch_path": "other/staging/_schema.yml",
                    "path": "staging/model_1.sql",
                },
                check_fails,
                id="patch_path_without_models_segment",
            ),
        ],
    )
    def test_check_model_documented_in_same_directory(self, model_override, check_fn):
        check_fn("check_model_documented_in_same_directory", model=model_override)

    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param({}, id="patch_path_absent"),
            pytest.param({"patch_path": None}, id="patch_path_none"),
            pytest.param({"patch_path": ""}, id="patch_path_empty_string"),
        ],
    )
    def test_undocumented_model_fails_with_the_directory_message(self, model_override):
        # Documents current behaviour: the "is not documented" branch is
        # unreachable because `clean_path_str` never returns None and the wrapped
        # resource always satisfies `hasattr(model, "patch_path")`. An
        # undocumented model therefore falls through to the directory comparison
        # and is reported as documented in a different (empty) directory.
        check_fails(
            "check_model_documented_in_same_directory",
            model={
                "original_file_path": "models/staging/model_1.sql",
                "path": "staging/model_1.sql",
                **model_override,
            },
            match=r"is documented in a different directory to the `\.sql` file: `` vs `models/staging`",
        )
