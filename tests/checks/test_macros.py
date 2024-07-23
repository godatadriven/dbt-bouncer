from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.check_macros import (
    check_macro_name_matches_file_name,
    check_populated_macro_arguments_description,
    check_populated_macro_description,
)


@pytest.mark.parametrize(
    "macro, expectation",
    [
        (
            {
                "name": "macro_1",
                "path": "macros/macro_1.sql",
                "unique_id": "model.package_name.macro_1",
            },
            does_not_raise(),
        ),
        (
            {
                "name": "my_macro_2",
                "path": "macros/macro_2.sql",
                "unique_id": "model.package_name.macro_2",
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
def test_check_populated_macro_arguments_description(macro, expectation):
    with expectation:
        check_populated_macro_arguments_description(macro=macro, request=None)


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
def test_check_populated_macro_description(macro, expectation):
    with expectation:
        check_populated_macro_description(macro=macro, request=None)
