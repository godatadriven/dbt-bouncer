from pathlib import Path

import pytest
import toml

from dbt_bouncer.utils import (
    flatten,
    get_dbt_bouncer_config,
    make_markdown_table,
    object_in_path,
)


def test_get_dbt_bouncer_config_commandline(tmp_path):
    config_file = tmp_path / "my_dbt_bouncer.yml"
    config_file.write_text("test: 1")
    config = get_dbt_bouncer_config(config_file=str(config_file), config_file_source="COMMANDLINE")
    assert config == {"test": 1}


def test_get_dbt_bouncer_config_default(tmp_path):
    config_file = tmp_path / "dbt_bouncer.yml"
    config_file.write_text("test: 1")
    config = get_dbt_bouncer_config(config_file=str(config_file), config_file_source="DEFAULT")
    assert config == {"test": 1}


PYPROJECT_TOML_SAMPLE_CONFIG = {
    "dbt_artifacts_dir": "dbt_project/target",
    "manifest_checks": [
        {"name": "check_model_description_populated"},
        {
            "include": "^marts",
            "upstream_path_pattern": "^staging|^intermediate",
            "name": "check_lineage_permitted_upstream_models",
        },
    ],
}


def test_get_dbt_bouncer_config_pyproject_toml(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    pyproject_file = tmp_path / "pyproject.toml"
    config = {"tool": {"dbt-bouncer": PYPROJECT_TOML_SAMPLE_CONFIG}}
    with Path.open(pyproject_file, "w") as f:
        toml.dump(config, f)

    config = get_dbt_bouncer_config(
        config_file=str("dbt_bouncer.yml"), config_file_source="DEFAULT"
    )

    assert config == PYPROJECT_TOML_SAMPLE_CONFIG


def test_get_dbt_bouncer_config_pyproject_toml_doesnt_exist(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    with pytest.raises(RuntimeError):
        config = get_dbt_bouncer_config(
            config_file=str("dbt_bouncer.yml"), config_file_source="DEFAULT"
        )


def test_get_dbt_bouncer_config_pyproject_toml_recursive(monkeypatch, tmp_path):
    monkeypatch.chdir(Path().cwd() / "tests/unit")

    pyproject_file = tmp_path / "pyproject.toml"
    config = {"tool": {"dbt-bouncer": PYPROJECT_TOML_SAMPLE_CONFIG}}
    with Path.open(pyproject_file, "w") as f:
        toml.dump(config, f)

    config = get_dbt_bouncer_config(
        config_file=str("dbt_bouncer.yml"), config_file_source="DEFAULT"
    )
    assert config == PYPROJECT_TOML_SAMPLE_CONFIG


def test_get_dbt_bouncer_config_pyproject_toml_no_bouncer_section(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    pyproject_file = tmp_path / "pyproject.toml"
    config = {"tool": {"dbt-bouncer-misspelled": PYPROJECT_TOML_SAMPLE_CONFIG}}
    with Path.open(pyproject_file, "w") as f:
        toml.dump(config, f)

    with pytest.raises(RuntimeError):
        config = get_dbt_bouncer_config(
            config_file=str("dbt_bouncer.yml"), config_file_source="DEFAULT"
        )


@pytest.mark.parametrize(
    "input, output",
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
def test_flatten(input, output):
    assert flatten(input) == output


@pytest.mark.parametrize(
    "input, output",
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
def test_make_markdown_table(input, output):
    assert make_markdown_table(input) == output


@pytest.mark.parametrize(
    "include_pattern, path, output",
    [
        ("^staging", "staging/model_1.sql", True),
        (None, "staging/model_1.sql", True),
        ("^staging", "model_1.sql", False),
        ("^staging", "intermediate/model_1.sql", False),
    ],
)
def test_object_in_path(include_pattern, path, output):
    assert object_in_path(include_pattern, path) == output
