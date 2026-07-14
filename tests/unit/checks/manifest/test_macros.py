import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckMacroArgumentsDescriptionPopulated:
    @pytest.mark.parametrize(
        ("macro_overrides", "check_fn"),
        [
            pytest.param(
                {
                    "arguments": [
                        {"name": "arg_1", "description": "This is arg_1."},
                        {"name": "arg_2", "description": "This is arg_2."},
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                },
                check_passes,
                id="valid_arguments",
            ),
            pytest.param(
                {
                    "arguments": [],
                    "macro_sql": "{% materialization udf, adapter=\"bigquery\" %}\n{%- set target = adapter.quote(this.database ~ '.' ~ this.schema ~ '.' ~ this.identifier) -%}\n{%- set parameter_list=config.get('parameter_list') -%}\n{%- set ret=config.get('returns') -%}\n{%- set description=config.get('description') -%}\n\n{%- set create_sql -%}\nCREATE OR REPLACE FUNCTION {{ target }}({{ parameter_list }})\nAS (\n  {{ sql }}\n);\n{%- endset -%}\n\n{% call statement('main') -%}\n  {{ create_sql }}\n{%- endcall %}\n\n{{ return({'relations': []}) }}\n\n{% endmaterialization %}",
                    "name": "materialization_udf",
                    "original_file_path": "macros/materialization_udf.sql",
                    "unique_id": "macro.package_name.materialization_udf",
                },
                check_passes,
                id="valid_materialization_no_args",
            ),
            pytest.param(
                {
                    "arguments": [
                        {"name": "arg_1", "description": "This is arg_1."},
                        {"name": "arg_2", "description": ""},
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                },
                check_fails,
                id="empty_description",
            ),
            pytest.param(
                {
                    "arguments": [
                        {"name": "arg_1", "description": "This is arg_1."},
                        {"name": "arg_2", "description": "                   "},
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                },
                check_fails,
                id="whitespace_description",
            ),
            pytest.param(
                {
                    "arguments": [
                        {"name": "arg_1", "description": "This is arg_1."},
                        {
                            "name": "arg_2",
                            "description": """
                            """,
                        },
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                },
                check_fails,
                id="multiline_whitespace_description",
            ),
            pytest.param(
                {
                    "arguments": [
                        {"name": "arg_1", "description": "This is arg_1."},
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                },
                check_fails,
                id="missing_argument_in_config",
            ),
            pytest.param(
                {
                    "arguments": [
                        {"name": "model", "description": "The model under test."},
                        {"name": "column_name", "description": "The column to check."},
                        {"name": "list_values", "description": "Allowed values."},
                    ],
                    # Test macro whose body is a single macro call: the `{% set %}`
                    # statement parses to a `jinja2.nodes.Assign` that has no `.nodes`
                    # attribute (see issue #927).
                    "macro_sql": '{% test expect_valid_length(model, column_name, list_values) %}\n{%- set expression = column_name ~ " in (" ~ list_values | join(\',\') ~ ")" -%}\n{{ dbt_expectations.expression_is_true(model, expression=expression) }}\n{% endtest %}',
                    "name": "test_expect_valid_length",
                    "original_file_path": "macros/expect_valid_length.sql",
                    "unique_id": "macro.package_name.test_expect_valid_length",
                },
                check_passes,
                id="test_macro_with_set_statement",
            ),
            pytest.param(
                {
                    "arguments": [
                        {"name": "model", "description": "The model under test."},
                        {"name": "column_name", "description": ""},
                        {"name": "list_values", "description": "Allowed values."},
                    ],
                    "macro_sql": '{% test expect_valid_length(model, column_name, list_values) %}\n{%- set expression = column_name ~ " in (" ~ list_values | join(\',\') ~ ")" -%}\n{{ dbt_expectations.expression_is_true(model, expression=expression) }}\n{% endtest %}',
                    "name": "test_expect_valid_length",
                    "original_file_path": "macros/expect_valid_length.sql",
                    "unique_id": "macro.package_name.test_expect_valid_length",
                },
                check_fails,
                id="test_macro_with_set_statement_missing_description",
            ),
            pytest.param(
                {
                    "arguments": [
                        {"name": "arg_1", "description": "n/a"},
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1) %} select {{ arg_1 }} from table {% endmacro %}",
                },
                check_fails,
                id="blocklisted_description_value",
            ),
            pytest.param(
                # No arguments are documented, so the check is a no-op even
                # though the macro SQL declares arguments (guarded by
                # `if macro.arguments:`).
                {
                    "arguments": [],
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                },
                check_passes,
                id="no_documented_arguments_is_noop",
            ),
        ],
    )
    def test_check_macro_arguments_description_populated(
        self, macro_overrides, check_fn
    ):
        check_fn(
            "check_macro_arguments_description_populated",
            macro=macro_overrides,
        )

    @pytest.mark.parametrize(
        ("macro_overrides", "min_description_length", "check_fn"),
        [
            pytest.param(
                {
                    "arguments": [
                        {
                            "name": "arg_1",
                            "description": "This description is long enough.",
                        },
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1) %} select {{ arg_1 }} from table {% endmacro %}",
                },
                25,
                check_passes,
                id="description_satisfies_custom_min_length",
            ),
            pytest.param(
                # "short" (5 chars) passes the default min length of 4 but
                # fails a stricter custom requirement.
                {
                    "arguments": [
                        {"name": "arg_1", "description": "short"},
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1) %} select {{ arg_1 }} from table {% endmacro %}",
                },
                25,
                check_fails,
                id="description_below_custom_min_length",
            ),
        ],
    )
    def test_check_macro_arguments_description_populated_min_length(
        self, macro_overrides, min_description_length, check_fn
    ):
        check_fn(
            "check_macro_arguments_description_populated",
            macro=macro_overrides,
            min_description_length=min_description_length,
        )


class TestCheckMacroCodeDoesNotContainRegexpPattern:
    @pytest.mark.parametrize(
        ("macro_overrides", "regexp_pattern", "check_fn"),
        [
            pytest.param(
                {
                    "arguments": [
                        {"name": "arg_1", "description": "This is arg_1."},
                        {"name": "arg_2", "description": "This is arg_2."},
                    ],
                    "macro_sql": "{% macro no_makes_sense(a, b) %} select coalesce({{ a }}, {{ b  }}) from table {% endmacro %}",
                },
                ".*[i][f][n][u][l][l].*",
                check_passes,
                id="does_not_contain_pattern",
            ),
            pytest.param(
                {
                    "arguments": [
                        {"name": "arg_1", "description": "This is arg_1."},
                        {"name": "arg_2", "description": "This is arg_2."},
                    ],
                    "macro_sql": "{% macro no_makes_sense(a, b) %} select ifnull({{ a }}, {{ b  }}) from table {% endmacro %}",
                },
                ".*[i][f][n][u][l][l].*",
                check_fails,
                id="contains_pattern",
            ),
            pytest.param(
                # The pattern is applied with `re.DOTALL`, so `.*` spans
                # newlines and matches a banned string on a later line.
                {
                    "macro_sql": "{% macro no_makes_sense(a, b) %}\n select ifnull({{ a }}, {{ b }}) from table \n{% endmacro %}",
                },
                ".*ifnull.*",
                check_fails,
                id="contains_pattern_across_newlines",
            ),
            pytest.param(
                # `re.match` is anchored at the start of the string, so an
                # unanchored pattern that only appears mid-string does NOT
                # match. Users must prefix with `.*` to match anywhere.
                {
                    "macro_sql": "{% macro no_makes_sense(a, b) %} select ifnull({{ a }}, {{ b }}) from table {% endmacro %}",
                },
                "ifnull",
                check_passes,
                id="unanchored_pattern_mid_string_does_not_match",
            ),
        ],
    )
    def test_check_macro_code_does_not_contain_regexp_pattern(
        self,
        macro_overrides,
        regexp_pattern,
        check_fn,
    ):
        check_fn(
            "check_macro_code_does_not_contain_regexp_pattern",
            macro=macro_overrides,
            regexp_pattern=regexp_pattern,
        )


class TestCheckMacroDescriptionPopulated:
    @pytest.mark.parametrize(
        ("macro_overrides", "check_fn"),
        [
            pytest.param(
                {"description": "This is macro_1.", "macro_sql": "select 1"},
                check_passes,
                id="valid_description",
            ),
            pytest.param(
                {"description": "", "macro_sql": "select 1"},
                check_fails,
                id="empty_description",
            ),
            pytest.param(
                {"description": "                ", "macro_sql": "select 1"},
                check_fails,
                id="whitespace_description",
            ),
            pytest.param(
                {
                    "description": """
                            """,
                    "macro_sql": "select 1",
                },
                check_fails,
                id="multiline_whitespace_description",
            ),
            pytest.param(
                # "n/a" is a non-empty placeholder that is explicitly treated
                # as unpopulated (alongside "none" and "null").
                {"description": "n/a", "macro_sql": "select 1"},
                check_fails,
                id="blocklisted_description_value",
            ),
        ],
    )
    def test_check_macro_description_populated(self, macro_overrides, check_fn):
        check_fn(
            "check_macro_description_populated",
            macro=macro_overrides,
        )

    @pytest.mark.parametrize(
        ("macro_overrides", "min_description_length", "check_fn"),
        [
            pytest.param(
                {
                    "description": "This is a sufficiently long description.",
                    "macro_sql": "select 1",
                },
                25,
                check_passes,
                id="description_satisfies_custom_min_length",
            ),
            pytest.param(
                # "A short one." (12 chars) passes the default min length of 4
                # but fails a stricter custom requirement.
                {"description": "A short one.", "macro_sql": "select 1"},
                25,
                check_fails,
                id="description_below_custom_min_length",
            ),
        ],
    )
    def test_check_macro_description_populated_min_length(
        self, macro_overrides, min_description_length, check_fn
    ):
        check_fn(
            "check_macro_description_populated",
            macro=macro_overrides,
            min_description_length=min_description_length,
        )


class TestCheckMacroHasMetaKeys:
    @pytest.mark.parametrize(
        ("keys", "macro_overrides"),
        [
            pytest.param(
                ["owner"],
                {"meta": {"owner": "Data Team"}},
                id="has_key",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"maturity": "high", "owner": "Data Team"}},
                id="has_key_with_others",
            ),
            pytest.param(
                ["owner", {"team": ["name", "slack"]}],
                {
                    "meta": {
                        "owner": "Data Team",
                        "team": {"name": "Analytics", "slack": "#analytics"},
                    },
                },
                id="has_nested_keys",
            ),
            pytest.param(
                [{"governance": [{"pii": ["level"]}]}],
                {"meta": {"governance": {"pii": {"level": "high"}}}},
                id="has_deeply_nested_keys",
            ),
        ],
    )
    def test_passes(self, keys, macro_overrides):
        check_passes("check_macro_has_meta_keys", keys=keys, macro=macro_overrides)

    @pytest.mark.parametrize(
        ("keys", "macro_overrides"),
        [
            pytest.param(
                ["owner"],
                {"meta": {}},
                id="empty_meta",
            ),
            pytest.param(
                ["owner"],
                {"meta": {"maturity": "high"}},
                id="missing_key",
            ),
            pytest.param(
                ["owner", {"team": ["name", "slack"]}],
                {"meta": {"owner": "Data Team", "team": {"name": "Analytics"}}},
                id="missing_nested_key",
            ),
            pytest.param(
                [{"governance": [{"pii": ["level"]}]}],
                {"meta": {"governance": {"pii": {"classified": True}}}},
                id="missing_deeply_nested_key",
            ),
        ],
    )
    def test_fails(self, keys, macro_overrides):
        check_fails("check_macro_has_meta_keys", keys=keys, macro=macro_overrides)


class TestCheckMacroMaxNumberOfLines:
    @pytest.mark.parametrize(
        ("macro_overrides", "max_number_of_lines", "check_fn"),
        [
            pytest.param(
                {
                    "macro_sql": '{% macro generate_schema_name(custom_schema_name, node) -%}\n    {#\n        Enter this block when run on stg or prd (except for CICD runs).\n        We want the same dataset and table names to be used across all environments.\n        For example, `marts.dim_customer` should exist in stg and prd, i.e. there should be no references to the project in the dataset name.\n        This will allow other tooling (BI, CICD scripts, etc.) to work across all environments without the need for differing logic per environment.\n    #}\n    {% if env_var("DBT_CICD_RUN", "false") == "true" %} {{ env_var("DBT_DATASET") }}\n\n    {% elif target.name in ["stg", "prd"] and env_var(\n        "DBT_CICD_RUN", "false"\n    ) == "false" %}\n\n        {{ node.config.schema }}\n\n    {% else %} {{ default__generate_schema_name(custom_schema_name, node) }}\n\n    {%- endif -%}\n\n{%- endmacro %}',
                },
                50,
                check_passes,
                id="within_limit",
            ),
            pytest.param(
                {
                    "macro_sql": '{% macro generate_schema_name(custom_schema_name, node) -%}\n    {#\n        Enter this block when run on stg or prd (except for CICD runs).\n        We want the same dataset and table names to be used across all environments.\n        For example, `marts.dim_customer` should exist in stg and prd, i.e. there should be no references to the project in the dataset name.\n        This will allow other tooling (BI, CICD scripts, etc.) to work across all environments without the need for differing logic per environment.\n    #}\n    {% if env_var("DBT_CICD_RUN", "false") == "true" %} {{ env_var("DBT_DATASET") }}\n\n    {% elif target.name in ["stg", "prd"] and env_var(\n        "DBT_CICD_RUN", "false"\n    ) == "false" %}\n\n        {{ node.config.schema }}\n\n    {% else %} {{ default__generate_schema_name(custom_schema_name, node) }}\n\n    {%- endif -%}\n\n{%- endmacro %}',
                    "name": "test_logic_2",
                    "original_file_path": "macros/test_logic_2.sql",
                    "path": "tests/test_logic_2.sql",
                    "unique_id": "macro.package_name.test_logic_2",
                },
                10,
                check_fails,
                id="exceeds_limit",
            ),
            pytest.param(
                # Exactly at the limit passes: the check only fails when the
                # count is strictly greater than the maximum.
                {"macro_sql": "line_1\nline_2\nline_3"},
                3,
                check_passes,
                id="exactly_at_limit",
            ),
            pytest.param(
                {"macro_sql": "line_1\nline_2\nline_3"},
                2,
                check_fails,
                id="one_over_limit",
            ),
        ],
    )
    def test_check_macro_max_number_of_lines(
        self, macro_overrides, max_number_of_lines, check_fn
    ):
        check_fn(
            "check_macro_max_number_of_lines",
            macro=macro_overrides,
            max_number_of_lines=max_number_of_lines,
        )

    @pytest.mark.parametrize(
        ("macro_overrides", "check_fn"),
        [
            pytest.param(
                {"macro_sql": "\n".join(["select 1"] * 100)},
                check_passes,
                id="default_limit_within",
            ),
            pytest.param(
                {"macro_sql": "\n".join(["select 1"] * 101)},
                check_fails,
                id="default_limit_exceeded",
            ),
        ],
    )
    def test_check_macro_max_number_of_lines_default(self, macro_overrides, check_fn):
        # The parameter is omitted so the default limit of 100 applies.
        check_fn(
            "check_macro_max_number_of_lines",
            macro=macro_overrides,
        )


class TestCheckMacroMaxNumberOfLinesInvalidParam:
    @pytest.mark.parametrize(
        "max_number_of_lines",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error(self, max_number_of_lines):
        from dbt_bouncer.testing import _run_check

        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_macro_max_number_of_lines",
                macro={"macro_sql": "select 1"},
                max_number_of_lines=max_number_of_lines,
            )


class TestCheckMacroMaxNumberOfArguments:
    @pytest.mark.parametrize(
        ("macro_overrides", "max_number_of_arguments", "check_fn"),
        [
            pytest.param(
                {
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                },
                5,
                check_passes,
                id="within_limit",
            ),
            pytest.param(
                {
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2, arg_3, arg_4, arg_5, arg_6) %} select 1 {% endmacro %}",
                },
                4,
                check_fails,
                id="exceeds_limit",
            ),
            pytest.param(
                # Exactly at the limit passes: the check only fails when the
                # count is strictly greater than the maximum.
                {
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2, arg_3) %} select 1 {% endmacro %}",
                },
                3,
                check_passes,
                id="exactly_at_limit",
            ),
            pytest.param(
                {
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2, arg_3) %} select 1 {% endmacro %}",
                },
                2,
                check_fails,
                id="one_over_limit",
            ),
            pytest.param(
                # Materializations declare no arguments, so they always pass.
                {
                    "macro_sql": "{% materialization udf, adapter=\"bigquery\" %}\n{{ return({'relations': []}) }}\n{% endmaterialization %}",
                    "name": "materialization_udf",
                    "original_file_path": "macros/materialization_udf.sql",
                    "unique_id": "macro.package_name.materialization_udf",
                },
                1,
                check_passes,
                id="materialization_has_no_arguments",
            ),
            pytest.param(
                # Generic tests are counted by their signature arguments,
                # including the implicit `model` argument (here: 3).
                {
                    "macro_sql": '{% test expect_valid_length(model, column_name, list_values) %}\n{{ dbt_expectations.expression_is_true(model, expression="1=1") }}\n{% endtest %}',
                    "name": "test_expect_valid_length",
                    "original_file_path": "macros/expect_valid_length.sql",
                    "unique_id": "macro.package_name.test_expect_valid_length",
                },
                2,
                check_fails,
                id="generic_test_counts_signature_arguments",
            ),
        ],
    )
    def test_check_macro_max_number_of_arguments(
        self, macro_overrides, max_number_of_arguments, check_fn
    ):
        check_fn(
            "check_macro_max_number_of_arguments",
            macro=macro_overrides,
            max_number_of_arguments=max_number_of_arguments,
        )

    @pytest.mark.parametrize(
        ("macro_overrides", "check_fn"),
        [
            pytest.param(
                {
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2, arg_3, arg_4) %} select 1 {% endmacro %}",
                },
                check_passes,
                id="default_limit_within",
            ),
            pytest.param(
                {
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2, arg_3, arg_4, arg_5) %} select 1 {% endmacro %}",
                },
                check_fails,
                id="default_limit_exceeded",
            ),
        ],
    )
    def test_check_macro_max_number_of_arguments_default(
        self, macro_overrides, check_fn
    ):
        # The parameter is omitted so the default limit of 4 applies.
        check_fn(
            "check_macro_max_number_of_arguments",
            macro=macro_overrides,
        )


class TestCheckMacroMaxNumberOfArgumentsInvalidParam:
    @pytest.mark.parametrize(
        "max_number_of_arguments",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error(self, max_number_of_arguments):
        from dbt_bouncer.testing import _run_check

        with pytest.raises(ValueError, match="greater than 0"):
            _run_check(
                "check_macro_max_number_of_arguments",
                macro={
                    "macro_sql": "{% macro no_makes_sense(arg_1) %} select {{ arg_1 }} {% endmacro %}"
                },
                max_number_of_arguments=max_number_of_arguments,
            )


class TestCheckMacroNameMatchesFileName:
    @pytest.mark.parametrize(
        ("macro_overrides", "check_fn"),
        [
            pytest.param(
                {},
                check_passes,
                id="matches_default",
            ),
            pytest.param(
                {
                    "name": "test_logic_1",
                    "original_file_path": "tests/logic_1.sql",
                    "path": "tests/logic_1.sql",
                    "unique_id": "macro.package_name.test_logic_1",
                },
                check_passes,
                id="matches_custom_path",
            ),
            pytest.param(
                {
                    "name": "my_macro_2",
                    "original_file_path": "macros/macro_2.sql",
                    "path": "macros/macro_2.sql",
                    "unique_id": "macro.package_name.macro_2",
                },
                check_fails,
                id="name_mismatch",
            ),
            pytest.param(
                {
                    "name": "test_logic_2",
                    "original_file_path": "macros/test_logic_2.sql",
                    "path": "tests/test_logic_2.sql",
                    "unique_id": "macro.package_name.test_logic_2",
                },
                check_fails,
                id="path_mismatch_in_object",
            ),
            pytest.param(
                # Windows-style backslash separators are normalised by
                # `clean_path_str` before the file stem is compared.
                {
                    "name": "macro_1",
                    "original_file_path": "macros\\subdir\\macro_1.sql",
                    "path": "macros\\subdir\\macro_1.sql",
                    "unique_id": "macro.package_name.macro_1",
                },
                check_passes,
                id="matches_windows_path",
            ),
        ],
    )
    def test_check_macro_name_matches_file_name(self, macro_overrides, check_fn):
        check_fn(
            "check_macro_name_matches_file_name",
            macro=macro_overrides,
        )


class TestCheckMacroNames:
    @pytest.mark.parametrize(
        ("macro_overrides", "include", "check_fn"),
        [
            pytest.param(
                {},
                "",
                check_passes,
                id="matches_pattern",
            ),
            pytest.param(
                {"name": "MyMacro"},
                "",
                check_fails,
                id="does_not_match_pattern",
            ),
            pytest.param(
                {},
                "^macros/",
                check_passes,
                id="matches_pattern_with_include",
            ),
        ],
    )
    def test_check_macro_names(self, macro_overrides, include, check_fn):
        check_fn(
            "check_macro_names",
            include=include,
            macro_name_pattern="^[a-z_0-9]+$",
            macro=macro_overrides,
        )

    @pytest.mark.parametrize(
        ("macro_overrides", "macro_name_pattern", "check_fn"),
        [
            pytest.param(
                # `re.match` anchors at the start but not the end, so a prefix
                # pattern matches a longer name.
                {"name": "finance_revenue"},
                "^finance_",
                check_passes,
                id="prefix_pattern_matches_longer_name",
            ),
            pytest.param(
                {"name": "revenue_finance"},
                "^finance_",
                check_fails,
                id="prefix_pattern_requires_start",
            ),
        ],
    )
    def test_check_macro_names_start_anchored(
        self, macro_overrides, macro_name_pattern, check_fn
    ):
        check_fn(
            "check_macro_names",
            macro_name_pattern=macro_name_pattern,
            macro=macro_overrides,
        )


class TestCheckMacroPropertyFileLocation:
    @pytest.mark.parametrize(
        ("macro_overrides", "check_fn"),
        [
            pytest.param(
                {"patch_path": "package_name://macros/_macros.yml"},
                check_passes,
                id="valid_underscore_prefix",
            ),
            pytest.param(
                {
                    "original_file_path": "macros/dir1/macro_1.sql",
                    "patch_path": "package_name://macros/dir1/_dir1__macros.yml",
                    "path": "macros/dir1/macro_1.sql",
                },
                check_passes,
                id="valid_nested_underscore_prefix",
            ),
            pytest.param(
                {
                    "name": "test_macro",
                    "original_file_path": "tests/generic/test_macro.sql",
                    "patch_path": "package_name://tests/generic/test_macro.yml",
                    "path": "tests/generic/test_macro.sql",
                    "unique_id": "macro.package_name.test_macro",
                },
                check_passes,
                id="valid_test_macro",
            ),
            pytest.param(
                {"patch_path": "package_name://macros/macros.yml"},
                check_fails,
                id="invalid_no_underscore",
            ),
            pytest.param(
                {
                    "original_file_path": "macros/dir1/macro_1.sql",
                    "patch_path": "package_name://macros/dir1/__macros.yml",
                    "path": "macros/dir1/macro_1.sql",
                },
                check_fails,
                id="invalid_double_underscore",
            ),
            pytest.param(
                {
                    "original_file_path": "macros/dir1/macro_1.sql",
                    "patch_path": None,
                    "path": "macros/dir1/macro_1.sql",
                },
                check_fails,
                id="missing_patch_path",
            ),
            pytest.param(
                # Two-level directory: the expected substring is `_dir1_dir2`
                # and the file must still end with `__macros.yml`.
                {
                    "original_file_path": "macros/dir1/dir2/macro_1.sql",
                    "patch_path": "package_name://macros/dir1/dir2/_dir1_dir2__macros.yml",
                    "path": "macros/dir1/dir2/macro_1.sql",
                },
                check_passes,
                id="valid_two_level_nested",
            ),
            pytest.param(
                # Underscore prefix and expected substring are present, but the
                # file does not end with `__macros.yml`.
                {
                    "original_file_path": "macros/dir1/macro_1.sql",
                    "patch_path": "package_name://macros/dir1/_dir1_macros.yml",
                    "path": "macros/dir1/macro_1.sql",
                },
                check_fails,
                id="invalid_wrong_suffix",
            ),
            pytest.param(
                # Underscore prefix is present but the expected `_dir1`
                # substring is missing.
                {
                    "original_file_path": "macros/dir1/macro_1.sql",
                    "patch_path": "package_name://macros/dir1/_other__macros.yml",
                    "path": "macros/dir1/macro_1.sql",
                },
                check_fails,
                id="invalid_missing_substring",
            ),
        ],
    )
    def test_check_macro_property_file_location(self, macro_overrides, check_fn):
        check_fn(
            "check_macro_property_file_location",
            macro=macro_overrides,
        )


class TestCheckMacroIsUsed:
    @pytest.mark.parametrize(
        ("macro_overrides", "ctx_overrides", "check_fn"),
        [
            pytest.param(
                {"unique_id": "macro.package_name.macro_1"},
                {
                    "nodes": {
                        "model.package_name.model_1": {
                            "depends_on": {"macros": ["macro.package_name.macro_1"]}
                        }
                    }
                },
                check_passes,
                id="used_by_model",
            ),
            pytest.param(
                {"unique_id": "macro.package_name.macro_1"},
                {
                    "macros": {
                        "macro.package_name.macro_2": {
                            "depends_on": {"macros": ["macro.package_name.macro_1"]}
                        }
                    }
                },
                check_passes,
                id="used_by_macro",
            ),
            pytest.param(
                {"unique_id": "macro.package_name.macro_1"},
                {
                    "exposures": {
                        "exposure.package_name.exposure_1": {
                            "depends_on": {"macros": ["macro.package_name.macro_1"]}
                        }
                    }
                },
                check_passes,
                id="used_by_exposure",
            ),
            pytest.param(
                {"unique_id": "macro.package_name.macro_1"},
                {
                    "unit_tests": {
                        "unit_test.package_name.model_1.unit_test_1": {
                            "depends_on": {"macros": ["macro.package_name.macro_1"]}
                        }
                    }
                },
                check_passes,
                id="used_by_unit_test",
            ),
            pytest.param(
                {
                    "name": "generate_schema_name",
                    "unique_id": "macro.my_project.generate_schema_name",
                    "depends_on": {
                        "macros": ["macro.dbt.default__generate_schema_name"]
                    },
                },
                {
                    "macros": {
                        "macro.dbt.generate_schema_name": {
                            "name": "generate_schema_name",
                            "package_name": "dbt",
                            "depends_on": {
                                "macros": ["macro.dbt.default__generate_schema_name"]
                            },
                        },
                        "macro.my_project.generate_schema_name": {
                            "name": "generate_schema_name",
                            "package_name": "my_project",
                            "unique_id": "macro.my_project.generate_schema_name",
                            "depends_on": {
                                "macros": ["macro.dbt.default__generate_schema_name"]
                            },
                        },
                    }
                },
                check_passes,
                id="overrides_dbt_builtin",
            ),
            pytest.param(
                {"unique_id": "macro.package_name.macro_1"},
                {
                    "nodes": {
                        "model.package_name.model_1": {"depends_on": {"macros": []}}
                    }
                },
                check_fails,
                id="unused",
            ),
        ],
    )
    def test_check_macro_is_used(self, macro_overrides, ctx_overrides, check_fn):
        check_fn(
            "check_macro_is_used",
            macro=macro_overrides,
            ctx_manifest_obj=ctx_overrides,
        )
