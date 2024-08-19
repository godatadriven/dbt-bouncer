from contextlib import nullcontext as does_not_raise

import pytest
from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Macros

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
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                }
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
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                }
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
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                }
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
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_arguments_description_populated(macro, expectation):
    with expectation:
        check_macro_arguments_description_populated(macro=macro, request=None)


@pytest.mark.parametrize(
    "macro, regexp_pattern, expectation",
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
                }
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
                }
            ),
            ".*[i][f][n][u][l][l].*",
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_code_does_not_contain_regexp_pattern(macro, regexp_pattern, expectation):
    with expectation:
        check_macro_code_does_not_contain_regexp_pattern(
            macro=macro, regexp_pattern=regexp_pattern, request=None
        )


@pytest.mark.parametrize(
    "macro, expectation",
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
                }
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
                }
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
                }
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
                }
            ),
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
            Macros(
                **{
                    "macro_sql": "select 1",
                    "name": "macro_1",
                    "original_file_path": "macros/macro_1.sql",
                    "package_name": "package_name",
                    "path": "macros/macro_1.sql",
                    "resource_type": "macro",
                    "unique_id": "macro.package_name.macro_1",
                }
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
                }
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
                }
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
                }
            ),
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
                }
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
                }
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
                }
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
                }
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
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_macro_property_file_location(macro, expectation):
    with expectation:
        check_macro_property_file_location(macro=macro, request=None)
