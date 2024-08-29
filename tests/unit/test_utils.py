import os
from pathlib import Path
from unittest import mock

import pytest
import toml

from dbt_bouncer.utils import (
    create_github_comment_file,
    flatten,
    get_config_file_path,
    load_config_file_contents,
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


def test_get_file_config_path_commandline(tmp_path):
    config_file = tmp_path / "my_dbt_bouncer.yml"
    config_file.write_text("test: 1")
    config_file_path = get_config_file_path(
        config_file=str(config_file),
        config_file_source="COMMANDLINE",
    )
    assert config_file_path == config_file


def test_get_file_config_path_default(tmp_path):
    config_file = tmp_path / "dbt_bouncer.yml"
    config_file.write_text("test: 1")
    config_file_path = get_config_file_path(
        config_file=str(config_file),
        config_file_source="DEFAULT",
    )
    assert config_file_path == config_file


PYPROJECT_TOML_SAMPLE_CONFIG = {
    "dbt_artifacts_dir": "dbt_project/target",
    "manifest_checks": [
        {"name": "check_model_has_unique_test"},
        {
            "include": "^staging",
            "model_name_pattern": "^stg_",
            "name": "check_model_names",
        },
    ],
}


def test_get_file_config_path_pyproject_toml(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    pyproject_file = tmp_path / "pyproject.toml"
    config = {"tool": {"dbt-bouncer": PYPROJECT_TOML_SAMPLE_CONFIG}}
    with Path.open(pyproject_file, "w") as f:
        toml.dump(config, f)

    config_file_path = get_config_file_path(
        config_file=str("dbt_bouncer.yml"),
        config_file_source="DEFAULT",
    )

    assert config_file_path == pyproject_file


def test_get_file_config_path_pyproject_toml_doesnt_exist(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    with pytest.raises(RuntimeError):
        get_config_file_path(
            config_file=str("dbt_bouncer.yml"),
            config_file_source="DEFAULT",
        )


def test_get_file_config_path_pyproject_toml_recursive(monkeypatch, tmp_path):
    Path.mkdir(tmp_path / "test")
    monkeypatch.chdir(tmp_path / "test")

    pyproject_file = tmp_path / "pyproject.toml"
    config = {"tool": {"dbt-bouncer": PYPROJECT_TOML_SAMPLE_CONFIG}}
    with Path.open(pyproject_file, "w") as f:
        toml.dump(config, f)

    config_file_path = get_config_file_path(
        config_file=str("dbt_bouncer.yml"),
        config_file_source="DEFAULT",
    )
    assert config_file_path == pyproject_file


def test_load_config_file_contents_pyproject_toml_no_bouncer_section(
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)

    pyproject_file = tmp_path / "pyproject.toml"
    config = {"tool": {"dbt-bouncer-misspelled": PYPROJECT_TOML_SAMPLE_CONFIG}}
    with Path.open(pyproject_file, "w") as f:
        toml.dump(config, f)

    with pytest.raises(RuntimeError):
        config = load_config_file_contents(config_file_path=tmp_path / "pyproject.toml")


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
