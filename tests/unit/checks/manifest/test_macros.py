from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import Macros
from dbt_bouncer.checks.manifest.check_macros import (
    CheckMacroArgumentsDescriptionPopulated,
    CheckMacroCodeDoesNotContainRegexpPattern,
    CheckMacroDescriptionPopulated,
    CheckMacroMaxNumberOfLines,
    CheckMacroNameMatchesFileName,
    CheckMacroPropertyFileLocation,
)

CheckMacroArgumentsDescriptionPopulated.model_rebuild()
CheckMacroCodeDoesNotContainRegexpPattern.model_rebuild()
CheckMacroDescriptionPopulated.model_rebuild()
CheckMacroMaxNumberOfLines.model_rebuild()
CheckMacroNameMatchesFileName.model_rebuild()
CheckMacroPropertyFileLocation.model_rebuild()


@pytest.mark.parametrize(
    ("macro", "expectation"),
    [
        (
            Macros(
                **{
                    "arguments": [
                        {
                            "name": "arg_1",
                            "description": "This is arg_1.",
                        },
                        {
                            "name": "arg_2",
                            "description": "This is arg_2.",
                        },
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            does_not_raise(),
        ),
        (
            Macros(
                **{
                    "arguments": [],
                    "macro_sql": "{% materialization udf, adapter=\"bigquery\" %}\n{%- set target = adapter.quote(this.database ~ '.' ~ this.schema ~ '.' ~ this.identifier) -%}\n\n{%- set parameter_list=config.get('parameter_list') -%}\n{%- set ret=config.get('returns') -%}\n{%- set description=config.get('description') -%}\n\n{%- set create_sql -%}\nCREATE OR REPLACE FUNCTION {{ target }}({{ parameter_list }})\nAS (\n  {{ sql }}\n);\n{%- endset -%}\n\n{% call statement('main') -%}\n  {{ create_sql }}\n{%- endcall %}\n\n{{ return({'relations': []}) }}\n\n{% endmaterialization %}",
                    "name": "materialization_udf",
                    "original_file_path": "macros/materialization_udf.sql",
                    "package_name": "package_name",
                    "path": "macros/materialization_udf.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.materialization_udf",
                },
            ),
            does_not_raise(),
        ),
        (
            Macros(
                **{
                    "arguments": [
                        {
                            "name": "arg_1",
                            "description": "This is arg_1.",
                        },
                        {
                            "name": "arg_2",
                            "description": "",
                        },
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
        (
            Macros(
                **{
                    "arguments": [
                        {
                            "name": "arg_1",
                            "description": "This is arg_1.",
                        },
                        {
                            "name": "arg_2",
                            "description": "                   ",
                        },
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
        (
            Macros(
                **{
                    "arguments": [
                        {
                            "name": "arg_1",
                            "description": "This is arg_1.",
                        },
                        {
                            "name": "arg_2",
                            "description": """
                            """,
                        },
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
        (
            Macros(
                **{
                    "arguments": [
                        {
                            "name": "arg_1",
                            "description": "This is arg_1.",
                        },
                    ],
                    "macro_sql": "{% macro no_makes_sense(arg_1, arg_2) %} select coalesce({{ arg_1 }}, {{ arg_2 }}) from table {% endmacro %}",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_arguments_description_populated(macro, expectation):
    with expectation:
        CheckMacroArgumentsDescriptionPopulated(
            macro=macro, name="check_macro_arguments_description_populated"
        ).execute()


@pytest.mark.parametrize(
    ("macro", "regexp_pattern", "expectation"),
    [
        (
            Macros(
                **{
                    "arguments": [
                        {
                            "name": "arg_1",
                            "description": "This is arg_1.",
                        },
                        {
                            "name": "arg_2",
                            "description": "This is arg_2.",
                        },
                    ],
                    "macro_sql": "{% macro no_makes_sense(a, b) %} select coalesce({{ a }}, {{ b  }}) from table {% endmacro %}",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            ".*[i][f][n][u][l][l].*",
            does_not_raise(),
        ),
        (
            Macros(
                **{
                    "arguments": [
                        {
                            "name": "arg_1",
                            "description": "This is arg_1.",
                        },
                        {
                            "name": "arg_2",
                            "description": "This is arg_2.",
                        },
                    ],
                    "macro_sql": "{% macro no_makes_sense(a, b) %} select ifnull({{ a }}, {{ b  }}) from table {% endmacro %}",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            ".*[i][f][n][u][l][l].*",
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_code_does_not_contain_regexp_pattern(
    macro,
    regexp_pattern,
    expectation,
):
    with expectation:
        CheckMacroCodeDoesNotContainRegexpPattern(
            macro=macro,
            name="check_macro_code_does_not_contain_regexp_pattern",
            regexp_pattern=regexp_pattern,
        ).execute()


@pytest.mark.parametrize(
    ("macro", "expectation"),
    [
        (
            Macros(
                **{
                    "description": "This is macro_1.",
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            does_not_raise(),
        ),
        (
            Macros(
                **{
                    "description": "",
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
        (
            Macros(
                **{
                    "description": "                ",
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
        (
            Macros(
                **{
                    "description": """
                            """,
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_description_populated(macro, expectation):
    with expectation:
        CheckMacroDescriptionPopulated(
            macro=macro,
            name="check_macro_description_populated",
        ).execute()


@pytest.mark.parametrize(
    ("macro", "max_number_of_lines", "expectation"),
    [
        (
            Macros(
                **{
                    "macro_sql": '{% macro generate_schema_name(custom_schema_name, node) -%}\n    {#\n        Enter this block when run on stg or prd (except for CICD runs).\n        We want the same dataset and table names to be used across all environments.\n        For example, `marts.dim_customer` should exist in stg and prd, i.e. there should be no references to the project in the dataset name.\n        This will allow other tooling (BI, CICD scripts, etc.) to work across all environments without the need for differing logic per environment.\n    #}\n    {% if env_var("DBT_CICD_RUN", "false") == "true" %} {{ env_var("DBT_DATASET") }}\n\n    {% elif target.name in ["stg", "prd"] and env_var(\n        "DBT_CICD_RUN", "false"\n    ) == "false" %}\n\n        {{ node.config.schema }}\n\n    {% else %} {{ default__generate_schema_name(custom_schema_name, node) }}\n\n    {%- endif -%}\n\n{%- endmacro %}',
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            50,
            does_not_raise(),
        ),
        (
            Macros(
                **{
                    "macro_sql": '{% macro generate_schema_name(custom_schema_name, node) -%}\n    {#\n        Enter this block when run on stg or prd (except for CICD runs).\n        We want the same dataset and table names to be used across all environments.\n        For example, `marts.dim_customer` should exist in stg and prd, i.e. there should be no references to the project in the dataset name.\n        This will allow other tooling (BI, CICD scripts, etc.) to work across all environments without the need for differing logic per environment.\n    #}\n    {% if env_var("DBT_CICD_RUN", "false") == "true" %} {{ env_var("DBT_DATASET") }}\n\n    {% elif target.name in ["stg", "prd"] and env_var(\n        "DBT_CICD_RUN", "false"\n    ) == "false" %}\n\n        {{ node.config.schema }}\n\n    {% else %} {{ default__generate_schema_name(custom_schema_name, node) }}\n\n    {%- endif -%}\n\n{%- endmacro %}',
                    "name": "test_logic_2",
                    "original_file_path": "macros/test_logic_2.sql",
                    "package_name": "package_name",
                    "path": "tests/test_logic_2.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.test_logic_2",
                },
            ),
            10,
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_max_number_of_lines(max_number_of_lines, macro, expectation):
    with expectation:
        CheckMacroMaxNumberOfLines(
            max_number_of_lines=max_number_of_lines,
            macro=macro,
            name="check_macro_max_number_of_lines",
        ).execute()


@pytest.mark.parametrize(
    ("macro", "expectation"),
    [
        (
            Macros(
                **{
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            does_not_raise(),
        ),
        (
            Macros(
                **{
                    "macro_sql": "select 1",
                    "name": "test_logic_1",
                    "original_file_path": "tests/logic_1.sql",
                    "package_name": "package_name",
                    "path": "tests/logic_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.test_logic_1",
                },
            ),
            does_not_raise(),
        ),
        (
            Macros(
                **{
                    "macro_sql": "select 1",
                    "name": "my_macro_2",
                    "original_file_path": "macros/macro_2.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_2.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_2",
                },
            ),
            pytest.raises(AssertionError),
        ),
        (
            Macros(
                **{
                    "macro_sql": "select 1",
                    "name": "test_logic_2",
                    "original_file_path": "macros/test_logic_2.sql",
                    "package_name": "package_name",
                    "path": "tests/test_logic_2.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.test_logic_2",
                },
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_name_matches_file_name(macro, expectation):
    with expectation:
        CheckMacroNameMatchesFileName(
            macro=macro,
            name="check_macro_name_matches_file_name",
        ).execute()


@pytest.mark.parametrize(
    ("macro", "expectation"),
    [
        (
            Macros(
                **{
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://macros/_macros.yml",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            does_not_raise(),
        ),
        (
            Macros(
                **{
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/dir1/macro_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://macros/dir1/_dir1__macros.yml",
                    "path": "macros/dir1/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            does_not_raise(),
        ),
        (
            Macros(
                **{
                    "macro_sql": "select 1",
                    "name": "test_macro",
                    "original_file_path": "tests/generic/test_macro.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://tests/generic/test_macro.yml",
                    "path": "tests/generic/test_macro.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.test_macro",
                },
            ),
            does_not_raise(),
        ),
        (
            Macros(
                **{
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://macros/macros.yml",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
        (
            Macros(
                **{
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/dir1/macro_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://macros/dir1/__macros.yml",
                    "path": "macros/dir1/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
        (
            Macros(
                **{
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/dir1/macro_1.sql",
                    "package_name": "package_name",
                    "patch_path": None,
                    "path": "macros/dir1/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                },
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_property_file_location(macro, expectation):
    with expectation:
        CheckMacroPropertyFileLocation(
            macro=macro,
            name="check_macro_property_file_location",
        ).execute()
