from contextlib import nullcontext as does_not_raise
from pathlib import Path

import click
import pytest
import toml
import yaml

from dbt_bouncer.config_file_validator import (
    get_config_file_path,
    load_config_file_contents,
    validate_conf,
)
from dbt_bouncer.main import cli


def test_get_file_config_path_commandline(tmp_path):
    config_file = tmp_path / "my_dbt_bouncer.yml"
    config_file.write_text("test: 1")
    config_file_path = get_config_file_path(
        config_file=str(config_file),
        config_file_source="COMMANDLINE",
    )

    assert config_file_path.replace("\\", "/") == config_file.as_posix().replace(
        "\\", "/"
    )


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


invalid_confs = [
    (
        f,
        pytest.raises(RuntimeError),
    )
    for f in Path("./tests/unit/config_files/invalid").glob("*.yml")
]


@pytest.mark.parametrize(
    ("f", "expectation"),
    invalid_confs,
    ids=[f.stem for f, _ in invalid_confs],
)
def test_validate_conf_invalid(f, expectation):
    with Path.open(f, "r") as fp:
        conf = yaml.safe_load(fp)

    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx as _, expectation as _:
        validate_conf(config_file_contents=conf)


def test_validate_conf_incorrect_name():
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx, pytest.raises(Exception) as excinfo:  # noqa: PT011
        validate_conf(
            config_file_contents={
                "manifest_checks": [{"name": "check_model_has_unique_tst"}]
            }
        )

    assert (
        str(excinfo.value)
        == "1. Check 'check_model_has_unique_tst' does not match any of the expected checks. Did you mean 'check_model_has_unique_test'?"
    )


def test_validate_conf_incorrect_names():
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx, pytest.raises(Exception) as excinfo:  # noqa: PT011
        validate_conf(
            config_file_contents={
                "manifest_checks": [
                    {"name": "check_model_has_unique_tst"},
                    {"name": "check_exposure_based_on_viw"},
                ]
            }
        )

    assert (
        str(excinfo.value)
        == """1. Check 'check_model_has_unique_tst' does not match any of the expected checks. Did you mean 'check_model_has_unique_test'?
2. Check 'check_exposure_based_on_viw' does not match any of the expected checks. Did you mean 'check_exposure_based_on_view'?"""
    )


valid_confs = [
    (
        f,
        does_not_raise(),
    )
    for f in Path("./tests/unit/config_files/valid").glob("*.yml")
]


@pytest.mark.parametrize(
    ("f", "expectation"),
    valid_confs,
    ids=[f.stem for f, _ in valid_confs],
)
def test_validate_conf_valid(f, expectation):
    with Path.open(f, "r") as fp:
        conf = yaml.safe_load(fp)

    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx, expectation:
        validate_conf(config_file_contents=conf)
