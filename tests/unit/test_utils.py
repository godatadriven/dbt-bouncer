import os
from unittest import mock

import pytest

from dbt_bouncer.utils import (
    create_github_comment_file,
    flatten,
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
        create_github_comment_file(failed_checks)
        assert (
            (tmp_path / "github-comment.md").read_text()
            == "## **Failed `dbt-bouncer`** checks\n\n\n| Check name | Failure message |\n| :--- | :--- |\n| check_model_description_populated | message_1 |\n| check_model_description_populated | message_2 |\n\n\nSent from this [GitHub Action workflow run](https://github.com/None/actions/runs/None)."
        )


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
        (None, "staging/model_1.sql", True),
        ("^staging", "model_1.sql", False),
        ("^staging", "intermediate/model_1.sql", False),
    ],
)
def test_object_in_path(include_pattern, path, output):
    assert object_in_path(include_pattern, path) == output
