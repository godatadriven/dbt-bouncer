import re

import pytest

from dbt_bouncer.testing import _run_check, check_fails, check_passes

_PYTHON_MODEL_BODY = (
    'import pandas as pd\n\ndef model(dbt, session):\n    df = dbt.ref("upstream")\n'
    "    return df"
)


class TestCheckModelCodeDoesNotContainRegexpPattern:
    @pytest.mark.parametrize(
        ("model_override", "regexp_pattern", "check_fn"),
        [
            pytest.param(
                {"raw_code": "select coalesce(a, b) from table"},
                ".*[i][f][n][u][l][l].*",
                check_passes,
                id="does_not_contain_pattern",
            ),
            pytest.param(
                {"raw_code": "select ifnull(a, b) from table"},
                ".*[i][f][n][u][l][l].*",
                check_fails,
                id="contains_pattern",
            ),
            # The pattern is compiled with re.DOTALL, so `.*` spans newlines.
            pytest.param(
                {"raw_code": "select\n    ifnull(a, b)\nfrom table"},
                ".*ifnull.*",
                check_fails,
                id="dotall_spans_newlines",
            ),
            pytest.param(
                {"raw_code": "select a from table"},
                "  .*[i][f][n][u][l][l].*  ",
                check_passes,
                id="padded_pattern_stripped",
            ),
            # `str(model.raw_code)` is matched, so a None raw_code becomes the
            # literal "None" rather than raising.
            pytest.param(
                {"raw_code": None},
                ".*ifnull.*",
                check_passes,
                id="none_raw_code",
            ),
            pytest.param(
                {"raw_code": None},
                "None",
                check_fails,
                id="none_raw_code_matched_as_string",
            ),
            # `re.match` anchors at the start, so an unanchored pattern must still
            # match from the beginning of the code.
            pytest.param(
                {"raw_code": "select ifnull(a, b)"},
                "ifnull",
                check_passes,
                id="pattern_not_anchored_at_start",
            ),
            pytest.param(
                {"language": "python", "raw_code": _PYTHON_MODEL_BODY},
                r"import\s+os",
                check_passes,
                id="python_no_forbidden_import",
            ),
            pytest.param(
                {
                    "language": "python",
                    "raw_code": f"import os\n{_PYTHON_MODEL_BODY}",
                },
                r".*import\s+os.*",
                check_fails,
                id="python_forbidden_import",
            ),
            pytest.param(
                {
                    "language": "python",
                    "raw_code": 'def model(dbt, session):\n    df = dbt.ref("large_table").to_pandas()\n    return df',
                },
                r".*\.to_pandas\(\)",
                check_fails,
                id="python_forbidden_method",
            ),
        ],
    )
    def test_check_model_code_does_not_contain_regexp_pattern(
        self, model_override, regexp_pattern, check_fn
    ):
        check_fn(
            "check_model_code_does_not_contain_regexp_pattern",
            model=model_override,
            regexp_pattern=regexp_pattern,
        )

    def test_invalid_regex_raises_re_error(self):
        check_fails(
            "check_model_code_does_not_contain_regexp_pattern",
            model={"raw_code": "select 1"},
            regexp_pattern="(unclosed",
            expected_exception=re.error,
            match="Invalid regex pattern",
        )


class TestCheckModelDoesNotUseCartesianJoin:
    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {
                    "raw_code": "SELECT a.id, b.name FROM table_a a JOIN table_b b ON a.id = b.id"
                },
                id="join_with_on",
            ),
            pytest.param(
                {
                    "raw_code": "SELECT a.id, b.name FROM table_a a JOIN table_b b USING (id)"
                },
                id="join_with_using",
            ),
            pytest.param(
                {"raw_code": "SELECT id FROM my_table"},
                id="single_table_select",
            ),
            pytest.param({"raw_code": ""}, id="empty_raw_code"),
            pytest.param({"raw_code": None}, id="none_raw_code"),
            pytest.param(
                {"language": "python", "raw_code": "import pandas as pd"},
                id="python_model",
            ),
            pytest.param(
                {"language": "python", "raw_code": "SELECT 1 FROM a CROSS JOIN b"},
                id="python_model_with_cross_join_text_skipped",
            ),
            pytest.param(
                {"raw_code": "-- CROSS JOIN old_table\nSELECT id FROM my_table"},
                id="cross_join_in_comment",
            ),
            pytest.param(
                # No `CROSS JOIN` keyword is present, so the regex fallback passes.
                {
                    "raw_code": "{# note\nSELECT a.id FROM table_a a JOIN table_b b ON a.id = b.id"
                },
                id="fallback_regex_scan_no_cross_join",
            ),
            # sqlglot represents `ON NULL` as an exp.Null whose `.this` is None,
            # which is neither a bool nor an Expression, so the condition is not
            # treated as constant.
            pytest.param(
                {"raw_code": "SELECT 1 FROM a JOIN b ON NULL"},
                id="join_on_null_is_not_a_constant_condition",
            ),
        ],
    )
    def test_passes(self, model_override):
        check_passes("check_model_does_not_use_cartesian_join", model=model_override)

    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"raw_code": "SELECT a.id, b.name FROM table_a a CROSS JOIN table_b b"},
                id="explicit_cross_join",
            ),
            pytest.param(
                {"raw_code": "SELECT a.id, b.name FROM table_a a JOIN table_b b"},
                id="join_without_on_or_using",
            ),
            pytest.param(
                {
                    "raw_code": "SELECT a.id, b.name FROM table_a a JOIN table_b b ON 1=1"
                },
                id="join_constant_on_1_equals_1",
            ),
            pytest.param(
                {
                    "raw_code": "SELECT a.id, b.name FROM table_a a JOIN table_b b ON TRUE"
                },
                id="join_constant_on_true",
            ),
            pytest.param(
                {"raw_code": "SELECT 1 FROM a JOIN b ON 0=0"},
                id="join_constant_on_0_equals_0",
            ),
            # Documents current behaviour: a NATURAL JOIN carries neither an `ON`
            # nor a `USING` clause, so it is reported as an accidental Cartesian
            # join even though it joins on the shared column names.
            pytest.param(
                {"raw_code": "SELECT 1 FROM a NATURAL JOIN b"},
                id="natural_join_reported_as_missing_clause",
            ),
            # Documents current behaviour: only the LEFT operand of the `ON`
            # expression is inspected, so a literal on the left is treated as a
            # constant condition even when the predicate is a genuine one. The
            # equivalent `ON b.id = 1` passes - see
            # test_literal_on_the_right_of_a_join_predicate_passes.
            pytest.param(
                {"raw_code": "SELECT 1 FROM a JOIN b ON 1 = b.id"},
                id="literal_on_the_left_of_a_join_predicate",
            ),
            pytest.param(
                # The regex fallback catches the `CROSS JOIN` keyword.
                {
                    "raw_code": "{# note\nSELECT a.id FROM table_a a CROSS JOIN table_b b"
                },
                id="fallback_regex_scan_cross_join",
            ),
        ],
    )
    def test_fails(self, model_override):
        check_fails("check_model_does_not_use_cartesian_join", model=model_override)

    @pytest.mark.parametrize(
        ("raw_code", "check_fn"),
        [
            pytest.param(
                "SELECT a.id, b.name FROM table_a a CROSS JOIN table_b b",
                check_passes,
                id="explicit_cross_join_allowed",
            ),
            # `allow_explicit_cross_join` also permits constant-`ON` joins, which
            # produce a Cartesian product just like an explicit `CROSS JOIN`.
            pytest.param(
                "SELECT a.id, b.name FROM table_a a JOIN table_b b ON 1=1",
                check_passes,
                id="constant_on_allowed",
            ),
            pytest.param(
                "{# note\nSELECT a.id FROM table_a a CROSS JOIN table_b b",
                check_passes,
                id="fallback_regex_scan_cross_join_allowed",
            ),
            # `allow_explicit_cross_join` does not cover a `JOIN` with no
            # `ON`/`USING` clause: an omitted clause is treated as an accidental
            # Cartesian join.
            pytest.param(
                "SELECT a.id, b.name FROM table_a a JOIN table_b b",
                check_fails,
                id="missing_clause_still_fails",
            ),
        ],
    )
    def test_allow_explicit_cross_join(self, raw_code, check_fn):
        check_fn(
            "check_model_does_not_use_cartesian_join",
            model={"raw_code": raw_code},
            allow_explicit_cross_join=True,
        )

    @pytest.mark.parametrize(
        ("raw_code", "match"),
        [
            pytest.param(
                "SELECT 1 FROM a CROSS JOIN b",
                r"uses an explicit `CROSS JOIN`\.",
                id="explicit_cross_join",
            ),
            pytest.param(
                "SELECT 1 FROM a JOIN b",
                r"uses a `JOIN` without an `ON` or `USING` clause\.",
                id="missing_clause",
            ),
            pytest.param(
                # The reported condition is only the LEFT operand of the `ON`
                # expression (`on_clause.this`), so `ON 1=1` renders as `ON 1`.
                "SELECT 1 FROM a JOIN b ON 1=1",
                r"uses a `JOIN` with a constant condition \(`ON 1`\)\.",
                id="constant_on",
            ),
            pytest.param(
                "{# note\nSELECT 1 FROM a CROSS JOIN b",
                r"uses a Cartesian or `CROSS JOIN`\.",
                id="fallback_regex_scan",
            ),
        ],
    )
    def test_failure_messages(self, raw_code, match):
        check_fails(
            "check_model_does_not_use_cartesian_join",
            model={"raw_code": raw_code},
            match=match,
        )

    @pytest.mark.parametrize(
        "raw_code",
        [
            pytest.param("SELECT 1 FROM a JOIN b ON b.id = 1", id="literal_on_right"),
            pytest.param(
                "SELECT 1 FROM a JOIN b ON a.id = b.id", id="columns_on_both_sides"
            ),
        ],
    )
    def test_literal_on_the_right_of_a_join_predicate_passes(self, raw_code):
        # Counterpart to the `literal_on_the_left_of_a_join_predicate` failing
        # case: the constant-condition test is asymmetric because it inspects
        # only `on_clause.this` (the left operand).
        check_passes(
            "check_model_does_not_use_cartesian_join", model={"raw_code": raw_code}
        )


class TestCheckModelDoesNotUseSelectStar:
    @pytest.mark.parametrize(
        ("model_override", "check_fn"),
        [
            pytest.param(
                {"raw_code": "SELECT id, name FROM my_table"},
                check_passes,
                id="explicit_columns",
            ),
            pytest.param({"raw_code": ""}, check_passes, id="empty_raw_code"),
            pytest.param({"raw_code": None}, check_passes, id="none_raw_code"),
            pytest.param(
                {"raw_code": "-- SELECT * FROM old_table\nSELECT id FROM my_table"},
                check_passes,
                id="select_star_only_in_line_comment",
            ),
            pytest.param(
                {"raw_code": "{# SELECT * FROM old_table #}\nSELECT id FROM my_table"},
                check_passes,
                id="select_star_only_in_jinja_comment",
            ),
            pytest.param(
                {"raw_code": "/* SELECT * FROM old_table */\nSELECT id FROM my_table"},
                check_passes,
                id="select_star_only_in_block_comment",
            ),
            pytest.param(
                {"raw_code": "SELECT count(*) FROM my_table"},
                check_passes,
                id="count_star_is_not_select_star",
            ),
            pytest.param(
                {"raw_code": "SELECT 'select * from x' AS note FROM my_table"},
                check_passes,
                id="select_star_only_in_string_literal",
            ),
            # Python models are not SQL, so the check skips them even when their
            # source contains a literal "SELECT *".
            pytest.param(
                {
                    "language": "python",
                    "raw_code": 'def model(dbt, session):\n    return dbt.ref("upstream")  # SELECT * FROM schema.table',
                },
                check_passes,
                id="python_model_skipped",
            ),
            # Unparseable SQL with no star falls through the regex fallback.
            pytest.param(
                {"raw_code": "{# note\nselect a from ("},
                check_passes,
                id="fallback_regex_scan_no_star",
            ),
            pytest.param(
                {"raw_code": "select * from my_table"},
                check_fails,
                id="lowercase_select_star",
            ),
            pytest.param(
                {"raw_code": "SELECT * FROM my_table"},
                check_fails,
                id="uppercase_select_star",
            ),
            pytest.param(
                {"raw_code": "SELECT  * FROM my_table"},
                check_fails,
                id="select_star_extra_space",
            ),
            pytest.param(
                {"raw_code": "WITH cte AS (SELECT id FROM t) SELECT * FROM cte"},
                check_fails,
                id="select_star_in_outer_query",
            ),
            pytest.param(
                {"raw_code": "SELECT DISTINCT * FROM my_table"},
                check_fails,
                id="select_distinct_star",
            ),
            pytest.param(
                {"raw_code": "SELECT ALL * FROM my_table"},
                check_fails,
                id="select_all_star",
            ),
            pytest.param(
                {"raw_code": "SELECT t.* FROM my_table AS t"},
                check_fails,
                id="qualified_star",
            ),
            pytest.param(
                {
                    "raw_code": "{% if true %}SELECT * FROM {{ ref('m') }}{% else %}SELECT * FROM {{ ref('n') }}{% endif %}",
                },
                check_fails,
                id="jinja_control_flow_falls_back_and_fails",
            ),
            pytest.param(
                {"raw_code": "{# note\nselect * from ("},
                check_fails,
                id="fallback_regex_scan_star",
            ),
        ],
    )
    def test_check_model_does_not_use_select_star(self, model_override, check_fn):
        check_fn("check_model_does_not_use_select_star", model=model_override)

    def test_failure_message(self):
        check_fails(
            "check_model_does_not_use_select_star",
            model={"raw_code": "SELECT * FROM my_table"},
            match=r"uses `SELECT \*`; list columns explicitly\.",
        )


class TestCheckModelHardCodedReferences:
    @pytest.mark.parametrize(
        ("model_override", "check_fn"),
        [
            pytest.param(
                {"raw_code": "SELECT * FROM {{ ref('other_model') }}"},
                check_passes,
                id="ref_jinja",
            ),
            pytest.param(
                {"raw_code": "SELECT * FROM {{ source('src', 'tbl') }}"},
                check_passes,
                id="source_jinja",
            ),
            pytest.param(
                {"raw_code": "WITH cte AS (SELECT 1) SELECT * FROM cte"},
                check_passes,
                id="cte_single_part",
            ),
            pytest.param(
                {
                    "raw_code": "{% if true %}SELECT a FROM {{ ref('m') }}{% else %}SELECT b FROM {{ ref('n') }}{% endif %}",
                },
                check_passes,
                id="jinja_control_flow_falls_back_and_passes",
            ),
            # Python models are not SQL, so the check skips them even when their
            # source contains a hard-coded dotted reference.
            pytest.param(
                {
                    "language": "python",
                    "raw_code": 'def model(dbt, session):\n    return session.sql("SELECT * FROM schema.table")',
                },
                check_passes,
                id="python_model_skipped",
            ),
            pytest.param(
                {"raw_code": "{# note\nselect a from ("},
                check_passes,
                id="fallback_regex_scan_no_reference",
            ),
            pytest.param(
                {"raw_code": "SELECT * FROM myschema.my_table"},
                check_fails,
                id="bare_schema_table",
            ),
            pytest.param(
                {
                    "raw_code": "SELECT a FROM t1 JOIN myschema.other_table ON t1.id = other_table.id",
                },
                check_fails,
                id="bare_join_schema_table",
            ),
            pytest.param(
                {"raw_code": "SELECT * FROM catalog.schema.table_name"},
                check_fails,
                id="three_part_identifier",
            ),
            pytest.param(
                {"raw_code": 'SELECT * FROM "myschema"."my_table"'},
                check_fails,
                id="quoted_schema_table",
            ),
            pytest.param(
                {"raw_code": "{# note\nselect a from sch.tbl where ("},
                check_fails,
                id="fallback_regex_scan_reference",
            ),
        ],
    )
    def test_check_model_hard_coded_references(self, model_override, check_fn):
        check_fn("check_model_hard_coded_references", model=model_override)

    def test_repeated_reference_is_reported_once(self):
        # The same qualified table referenced twice is de-duplicated, so it is
        # listed a single time.
        check_fails(
            "check_model_hard_coded_references",
            model={
                "raw_code": "SELECT a FROM sch.tbl WHERE a IN (SELECT b FROM sch.tbl)"
            },
            match=r"hard-coded table references: \['sch\.tbl'\]\.",
        )


class TestCheckModelHasSemiColon:
    @pytest.mark.parametrize(
        ("model_override", "check_fn"),
        [
            pytest.param(
                {"raw_code": "select 1 as id"}, check_passes, id="no_semicolon"
            ),
            pytest.param(
                {"raw_code": "select 1 as id\n                    "},
                check_passes,
                id="multiline_no_semicolon",
            ),
            pytest.param(
                {"raw_code": "-- comment with ;\n                    select 1 as id"},
                check_passes,
                id="semicolon_in_comment",
            ),
            pytest.param({"raw_code": ""}, check_passes, id="empty_raw_code"),
            pytest.param({"raw_code": None}, check_passes, id="none_raw_code"),
            pytest.param(
                {
                    "language": "python",
                    "raw_code": 'def model(dbt, session):\n    df = dbt.ref("upstream")\n    return df',
                },
                check_passes,
                id="python_no_semicolon",
            ),
            pytest.param({"raw_code": "select 1 as id;"}, check_fails, id="semicolon"),
            pytest.param(
                {"raw_code": "select 1 as id; "},
                check_fails,
                id="semicolon_with_space",
            ),
            pytest.param(
                {"raw_code": "select 1 as id;\n\n                    "},
                check_fails,
                id="multiline_semicolon",
            ),
            pytest.param(
                {"raw_code": "select 1 as id\n                    ; "},
                check_fails,
                id="multiline_semicolon_next_line",
            ),
            # The check is language-agnostic: it only inspects the last
            # non-whitespace character.
            pytest.param(
                {
                    "language": "python",
                    "raw_code": 'def model(dbt, session):\n    return dbt.ref("upstream");',
                },
                check_fails,
                id="python_semicolon",
            ),
        ],
    )
    def test_check_model_has_semi_colon(self, model_override, check_fn):
        check_fn("check_model_has_semi_colon", model=model_override)

    def test_failure_message(self):
        check_fails(
            "check_model_has_semi_colon",
            model={"raw_code": "select 1 as id;"},
            match=r"ends with a semi-colon, this is not permitted\.",
        )


class TestCheckModelIncrementalHasUniqueKey:
    @pytest.mark.parametrize(
        ("model_override", "check_fn"),
        [
            pytest.param(
                {"config": {"materialized": "incremental", "unique_key": "id"}},
                check_passes,
                id="incremental_with_unique_key",
            ),
            pytest.param(
                {
                    "config": {
                        "materialized": "incremental",
                        "unique_key": ["id", "day"],
                    }
                },
                check_passes,
                id="incremental_with_composite_unique_key",
            ),
            pytest.param(
                {"config": {"materialized": "view"}},
                check_passes,
                id="view_no_unique_key",
            ),
            pytest.param(
                {"config": {"materialized": "table"}},
                check_passes,
                id="table_no_unique_key",
            ),
            pytest.param({"config": None}, check_passes, id="config_none"),
            pytest.param({}, check_passes, id="config_absent"),
            pytest.param(
                {"config": {"materialized": "incremental"}},
                check_fails,
                id="incremental_no_unique_key",
            ),
            pytest.param(
                {"config": {"materialized": "incremental", "unique_key": ""}},
                check_fails,
                id="incremental_empty_unique_key",
            ),
            pytest.param(
                {"config": {"materialized": "incremental", "unique_key": None}},
                check_fails,
                id="incremental_none_unique_key",
            ),
            pytest.param(
                {"config": {"materialized": "incremental", "unique_key": []}},
                check_fails,
                id="incremental_empty_list_unique_key",
            ),
        ],
    )
    def test_check_model_incremental_has_unique_key(self, model_override, check_fn):
        check_fn("check_model_incremental_has_unique_key", model=model_override)

    def test_failure_message(self):
        check_fails(
            "check_model_incremental_has_unique_key",
            model={"config": {"materialized": "incremental"}},
            match=r"is incremental but has no `unique_key`\.",
        )


class TestCheckModelMaterializationPermitted:
    @pytest.mark.parametrize(
        ("model_override", "permitted_materializations", "check_fn"),
        [
            pytest.param(
                {"config": {"materialized": "view"}},
                ["view"],
                check_passes,
                id="view_permitted_view",
            ),
            pytest.param(
                {"config": {"materialized": "table"}},
                ["table"],
                check_passes,
                id="table_permitted_table",
            ),
            pytest.param(
                {"config": {"materialized": "ephemeral"}},
                ["ephemeral", "view"],
                check_passes,
                id="ephemeral_permitted_multiple",
            ),
            pytest.param(
                {"config": {"materialized": "incremental"}},
                ["incremental"],
                check_passes,
                id="incremental_permitted",
            ),
            pytest.param(
                {"config": {"materialized": "table"}},
                ["view"],
                check_fails,
                id="table_overridden_from_view",
            ),
            pytest.param(
                {"config": {"materialized": "incremental"}},
                ["view", "table"],
                check_fails,
                id="incremental_not_permitted",
            ),
            pytest.param({"config": None}, ["view"], check_fails, id="none_config"),
            pytest.param(
                {"config": {"materialized": "view"}},
                [],
                check_fails,
                id="nothing_permitted",
            ),
        ],
    )
    def test_check_model_materialization_permitted(
        self, model_override, permitted_materializations, check_fn
    ):
        check_fn(
            "check_model_materialization_permitted",
            model=model_override,
            permitted_materializations=permitted_materializations,
        )

    def test_failure_message_lists_the_permitted_materializations(self):
        check_fails(
            "check_model_materialization_permitted",
            model={"config": {"materialized": "table"}},
            permitted_materializations=["view", "ephemeral"],
            match=(
                r"is materialized as `table`, expected one of "
                r"\['view', 'ephemeral'\]\."
            ),
        )

    def test_invalid_materialization_rejected(self):
        with pytest.raises(ValueError, match="'ephemeral'"):
            _run_check(
                "check_model_materialization_permitted",
                model={"config": {"materialized": "view"}},
                permitted_materializations=["not_a_materialization"],
            )


_LONG_RAW_CODE = 'with\n    source as (\n\n        {#-\n    Normally we would select from the table here, but we are using seeds to load\n    our data in this project\n    #}\n        select * from {{ ref("raw_orders") }}\n\n    ),\n\n    renamed as (\n\n        select id as order_id, user_id as customer_id, order_date, status from source\n\n    )\n\nselect *\nfrom renamed'


class TestCheckModelMaxNumberOfLines:
    @pytest.mark.parametrize(
        ("model_override", "max_number_of_lines", "check_fn"),
        [
            pytest.param(
                {"raw_code": _LONG_RAW_CODE}, 100, check_passes, id="within_limit"
            ),
            pytest.param(
                {"raw_code": _LONG_RAW_CODE}, 10, check_fails, id="exceeds_limit"
            ),
            # A single line with no trailing newline counts as 1.
            pytest.param(
                {"raw_code": "select 1"}, 1, check_passes, id="single_line_at_limit"
            ),
            pytest.param(
                {"raw_code": "select 1\nfrom t"},
                1,
                check_fails,
                id="two_lines_one_above_limit",
            ),
            pytest.param(
                {"raw_code": "select 1\nfrom t"},
                2,
                check_passes,
                id="two_lines_at_limit",
            ),
            # A None raw_code counts as 1 line rather than raising.
            pytest.param({"raw_code": None}, 1, check_passes, id="none_raw_code"),
            pytest.param({"raw_code": ""}, 1, check_passes, id="empty_raw_code"),
            pytest.param(
                {"language": "python", "raw_code": _PYTHON_MODEL_BODY},
                10,
                check_passes,
                id="python_within_limit",
            ),
            pytest.param(
                {
                    "language": "python",
                    "raw_code": "import pandas as pd\n# Line 2\n# Line 3\n# Line 4\n# Line 5\n# Line 6\n# Line 7\n# Line 8\n# Line 9\n# Line 10\n# Line 11\n\n"
                    + _PYTHON_MODEL_BODY,
                },
                10,
                check_fails,
                id="python_exceeds_limit",
            ),
        ],
    )
    def test_check_model_max_number_of_lines(
        self, model_override, max_number_of_lines, check_fn
    ):
        check_fn(
            "check_model_max_number_of_lines",
            max_number_of_lines=max_number_of_lines,
            model=model_override,
        )

    def test_failure_message_reports_both_counts(self):
        check_fails(
            "check_model_max_number_of_lines",
            max_number_of_lines=1,
            model={"raw_code": "select 1\nfrom t"},
            match=(
                r"has 2 lines, this is more than the maximum permitted number of "
                r"lines \(1\)\."
            ),
        )

    @pytest.mark.parametrize(
        "max_number_of_lines",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error(self, max_number_of_lines):
        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_model_max_number_of_lines",
                max_number_of_lines=max_number_of_lines,
                model={"raw_code": "select 1"},
            )
