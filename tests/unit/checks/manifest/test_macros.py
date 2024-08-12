from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.manifest.check_macros import (
    check_macro_arguments_description_populated,
    check_macro_code_does_not_contain_regexp_pattern,
    check_macro_description_populated,
    check_macro_name_matches_file_name,
    check_macro_property_file_location,
)


@pytest.mark.parametrize(
    "macro, expectation",
    [
        (
            {
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
                "name": "macro_1",
                "unique_id": "model.package_name.macro_1",
            },
            does_not_raise(),
        ),
        (
            {
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
                "name": "macro_2",
                "unique_id": "model.package_name.macro_2",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "arguments": [
                    {
                        "name": "arg_1",
                        "description": "This is arg_1.",
                    },
                    {
                        "name": "arg_2",
                        "description": "     ",
                    },
                ],
                "name": "macro_3",
                "unique_id": "model.package_name.macro_3",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
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
                "name": "macro_4",
                "unique_id": "model.package_name.macro_4",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_arguments_description_populated(macro, expectation):
    with expectation:
        check_macro_arguments_description_populated(macro=macro, request=None)


@pytest.mark.parametrize(
    "check_config, macro, expectation",
    [
        (
            {"regexp_pattern": ".*[i][f][n][u][l][l].*"},
            {
                "macro_sql": "{% macro no_makes_sense(a, b) %} select coalesce({{ a }}, {{ b  }}) from table {% endmacro %}",
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "regexp_pattern": ".*[i][f][n][u][l][l].*",
            },
            {
                "macro_sql": "{% macro no_makes_sense(a, b) %} select ifnull({{ a }}, {{ b  }}) from table {% endmacro %}",
                "unique_id": "model.package_name.stg_model_2",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_code_does_not_contain_regexp_pattern(check_config, macro, expectation):
    with expectation:
        check_macro_code_does_not_contain_regexp_pattern(
            check_config=check_config, macro=macro, request=None
        )


@pytest.mark.parametrize(
    "macro, expectation",
    [
        (
            {
                "description": "This is macro_1.",
                "name": "macro_1",
                "unique_id": "model.package_name.macro_1",
            },
            does_not_raise(),
        ),
        (
            {
                "description": "",
                "name": "macro_2",
                "unique_id": "model.package_name.macro_2",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": "     ",
                "name": "macro_3",
                "unique_id": "model.package_name.macro_3",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": """
                        """,
                "name": "macro_4",
                "unique_id": "model.package_name.macro_4",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_description_populated(macro, expectation):
    with expectation:
        check_macro_description_populated(macro=macro, request=None)


@pytest.mark.parametrize(
    "macro, expectation",
    [
        (
            {
                "name": "macro_1",
                "path": "macros/macro_1.sql",
                "unique_id": "macro.package_name.macro_1",
            },
            does_not_raise(),
        ),
        (
            {
                "name": "test_logic_1",
                "path": "tests/logic_1.sql",
                "unique_id": "macro.package_name.test_logic_1",
            },
            does_not_raise(),
        ),
        (
            {
                "name": "my_macro_2",
                "path": "macros/macro_2.sql",
                "unique_id": "macro.package_name.macro_2",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "name": "test_logic_2",
                "path": "tests/my_logic_1.sql",
                "unique_id": "macro.package_name.test_logic_2",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_name_matches_file_name(macro, expectation):
    with expectation:
        check_macro_name_matches_file_name(macro=macro, request=None)


@pytest.mark.parametrize(
    "macro, expectation",
    [
        (
            {
                "name": "macro_1",
                "patch_path": "package_name://macros/_macros.yml",
                "path": "macros/macro_1.sql",
                "unique_id": "macro.package_name.macro_1",
            },
            does_not_raise(),
        ),
        (
            {
                "name": "macro_2",
                "patch_path": "package_name://macros/dir1/_dir1__macros.yml",
                "path": "macros/dir1/macro_2.sql",
                "unique_id": "macro.package_name.macro_2",
            },
            does_not_raise(),
        ),
        (
            {
                "name": "macro_3",
                "patch_path": "package_name://macros/macros.yml",
                "path": "macros/macro_3.sql",
                "unique_id": "macro.package_name.macro_3",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "name": "macro_4",
                "patch_path": "package_name://macros/dir1/__macros.yml",
                "path": "macros/dir1/macro_4.sql",
                "unique_id": "macro.package_name.macro_4",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_property_file_location(macro, expectation):
    with expectation:
        check_macro_property_file_location(macro=macro, request=None)
