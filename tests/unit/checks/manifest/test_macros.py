import pytest

from dbt_bouncer.testing import check_fails, check_passes


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
    ],
)
def test_check_macro_arguments_description_populated(macro_overrides, check_fn):
    check_fn(
        "check_macro_arguments_description_populated",
        macro=macro_overrides,
    )


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
    ],
)
def test_check_macro_code_does_not_contain_regexp_pattern(
    macro_overrides,
    regexp_pattern,
    check_fn,
):
    check_fn(
        "check_macro_code_does_not_contain_regexp_pattern",
        macro=macro_overrides,
        regexp_pattern=regexp_pattern,
    )


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
    ],
)
def test_check_macro_description_populated(macro_overrides, check_fn):
    check_fn(
        "check_macro_description_populated",
        macro=macro_overrides,
    )


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
    ],
)
def test_check_macro_max_number_of_lines(
    macro_overrides, max_number_of_lines, check_fn
):
    check_fn(
        "check_macro_max_number_of_lines",
        macro=macro_overrides,
        max_number_of_lines=max_number_of_lines,
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
    ],
)
def test_check_macro_name_matches_file_name(macro_overrides, check_fn):
    check_fn(
        "check_macro_name_matches_file_name",
        macro=macro_overrides,
    )


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
    ],
)
def test_check_macro_property_file_location(macro_overrides, check_fn):
    check_fn(
        "check_macro_property_file_location",
        macro=macro_overrides,
    )


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
            {"nodes": {"model.package_name.model_1": {"depends_on": {"macros": []}}}},
            check_fails,
            id="unused",
        ),
    ],
)
def test_check_macro_is_unused(macro_overrides, ctx_overrides, check_fn):
    check_fn(
        "check_macro_is_unused",
        macro=macro_overrides,
        ctx_manifest_obj=ctx_overrides,
    )
