import os
from unittest import mock

import pytest

from dbt_bouncer.utils import (
    create_github_comment_file,
    flatten,
    get_clean_model_name,
    get_package_version_number,
    make_markdown_table,
    object_in_path,
)


def test_create_github_comment_file(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    with mock.patch.dict(os.environ, clear=True):
        failed_checks = [
            ["check_model_description_populated", "message_1"],
            ["check_model_description_populated", "message_2"],
        ]
        create_github_comment_file(failed_checks, show_all_failures=False)
        assert (
            (tmp_path / "github-comment.md").read_text()
            == "## **Failed `dbt-bouncer`** checks\n\n\n| Check name | Failure message |\n| :--- | :--- |\n| check_model_description_populated | message_1 |\n| check_model_description_populated | message_2 |\n\n\nSent from this [GitHub Action workflow run](https://github.com/None/actions/runs/None)."
        )


def test_create_github_comment_file_show_all_failures_false(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    with mock.patch.dict(os.environ, clear=True):
        failed_checks = [
            ["check_model_description_populated", "message_1"],
            ["check_model_description_populated", "message_2"],
            ["check_model_description_populated", "message_3"],
            ["check_model_description_populated", "message_4"],
            ["check_model_description_populated", "message_5"],
            ["check_model_description_populated", "message_6"],
            ["check_model_description_populated", "message_7"],
            ["check_model_description_populated", "message_8"],
            ["check_model_description_populated", "message_9"],
            ["check_model_description_populated", "message_10"],
            ["check_model_description_populated", "message_11"],
            ["check_model_description_populated", "message_12"],
            ["check_model_description_populated", "message_13"],
            ["check_model_description_populated", "message_14"],
            ["check_model_description_populated", "message_15"],
            ["check_model_description_populated", "message_16"],
            ["check_model_description_populated", "message_17"],
            ["check_model_description_populated", "message_18"],
            ["check_model_description_populated", "message_19"],
            ["check_model_description_populated", "message_20"],
            ["check_model_description_populated", "message_21"],
            ["check_model_description_populated", "message_22"],
            ["check_model_description_populated", "message_23"],
            ["check_model_description_populated", "message_24"],
            ["check_model_description_populated", "message_25"],
            ["check_model_description_populated", "message_26"],
            ["check_model_description_populated", "message_27"],
            ["check_model_description_populated", "message_28"],
            ["check_model_description_populated", "message_29"],
            ["check_model_description_populated", "message_30"],
        ]
        create_github_comment_file(failed_checks, show_all_failures=False)
        assert len((tmp_path / "github-comment.md").read_text().split("\n")) == 35


def test_create_github_comment_file_show_all_failures_true(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    with mock.patch.dict(os.environ, clear=True):
        failed_checks = [
            ["check_model_description_populated", "message_1"],
            ["check_model_description_populated", "message_2"],
            ["check_model_description_populated", "message_3"],
            ["check_model_description_populated", "message_4"],
            ["check_model_description_populated", "message_5"],
            ["check_model_description_populated", "message_6"],
            ["check_model_description_populated", "message_7"],
            ["check_model_description_populated", "message_8"],
            ["check_model_description_populated", "message_9"],
            ["check_model_description_populated", "message_10"],
            ["check_model_description_populated", "message_11"],
            ["check_model_description_populated", "message_12"],
            ["check_model_description_populated", "message_13"],
            ["check_model_description_populated", "message_14"],
            ["check_model_description_populated", "message_15"],
            ["check_model_description_populated", "message_16"],
            ["check_model_description_populated", "message_17"],
            ["check_model_description_populated", "message_18"],
            ["check_model_description_populated", "message_19"],
            ["check_model_description_populated", "message_20"],
            ["check_model_description_populated", "message_21"],
            ["check_model_description_populated", "message_22"],
            ["check_model_description_populated", "message_23"],
            ["check_model_description_populated", "message_24"],
            ["check_model_description_populated", "message_25"],
            ["check_model_description_populated", "message_26"],
            ["check_model_description_populated", "message_27"],
            ["check_model_description_populated", "message_28"],
            ["check_model_description_populated", "message_29"],
            ["check_model_description_populated", "message_30"],
        ]
        create_github_comment_file(failed_checks, show_all_failures=True)
        assert len((tmp_path / "github-comment.md").read_text().split("\n")) == 38


@pytest.mark.parametrize(
    ("unique_id", "expected_model_name"),
    [
        ("model.my_project.my_model", "my_model"),
        ("model.my_project.my_model.v1", "my_model_v1"),
    ],
)
def test_get_clean_model_name(unique_id, expected_model_name):
    assert get_clean_model_name(unique_id) == expected_model_name


@pytest.mark.parametrize(
    ("data_in", "data_out"),
    [
        (
            {"a": 1, "b": 2},
            {">>a": 1, ">>b": 2},
        ),
        (
            {"a": 1, "b": {"c": 3, "d": 4}},
            {">>a": 1, ">>b>c": 3, ">>b>d": 4},
        ),
        (
            {"a": 1, "b": [{"c": 3, "d": 4}]},
            {">>a": 1, ">>b>0>c": 3, ">>b>0>d": 4},
        ),
    ],
)
def test_flatten(data_in, data_out):
    assert flatten(data_in) == data_out


@pytest.mark.parametrize(
    ("version_a", "version_b", "expectation"),
    [
        ("1.6.0", "1.6.0", True),
        ("1.6.1", "1.6.0", True),
        ("1.5.9", "1.6.0", False),
        ("2024.11.06+2a3d725", "1.6.0", True),
    ],
)
def test_get_package_version_number(version_a, version_b, expectation):
    assert (
        get_package_version_number(version_a) >= get_package_version_number(version_b)
    ) == expectation


@pytest.mark.parametrize(
    ("data_in", "data_out"),
    [
        (
            [
                [
                    "col_1",
                ],
                [
                    "a",
                ],
                [
                    "b",
                ],
            ],
            """
| col_1 |
| :--- |
| a |
| b |
""",
        ),
        (
            [
                [
                    "col_1",
                    "col_2",
                ],
                [
                    "a",
                    "1",
                ],
                [
                    "b",
                    "2",
                ],
            ],
            """
| col_1 | col_2 |
| :--- | :--- |
| a | 1 |
| b | 2 |
""",
        ),
        (
            [
                [
                    "col_1",
                    "col_2",
                    "col_3",
                ],
                [
                    "a",
                    "1",
                ],
                [
                    "b",
                    "2",
                ],
                [
                    "c",
                    "3",
                ],
            ],
            """
| col_1 | col_2 | col_3 |
| :--- | :--- | :--- |
| a | 1 |
| b | 2 |
| c | 3 |
""",
        ),
    ],
)
def test_make_markdown_table(data_in, data_out):
    assert make_markdown_table(data_in) == data_out


@pytest.mark.parametrize(
    ("include_pattern", "path", "output"),
    [
        ("^staging", "staging/model_1.sql", True),
        ("^staging", "staging\\model_1.sql", True),  # Windows path format
        (None, "staging/model_1.sql", True),
        ("^staging", "model_1.sql", False),
        ("^staging", "intermediate/model_1.sql", False),
    ],
)
def test_object_in_path(include_pattern, path, output):
    assert object_in_path(include_pattern, path) == output
