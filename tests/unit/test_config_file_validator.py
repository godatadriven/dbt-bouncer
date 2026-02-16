import os
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from unittest import mock

import click
import pytest
import yaml
from pydantic import PydanticUserError

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


def test_get_file_config_path_env_var(tmp_path):
    config_file = tmp_path / "dbt_bouncer.yml"
    config_file.write_text("test: 1")

    with mock.patch.dict(os.environ, clear=True) and pytest.MonkeyPatch.context() as mp:
        custom_config_file_path = "/dir/my_custom_config_file.yml"
        mp.setenv("DBT_BOUNCER_CONFIG_FILE", custom_config_file_path)

        config_file_path = get_config_file_path(
            config_file=str(config_file),
            config_file_source="DEFAULT",
        )

    assert config_file_path == Path(custom_config_file_path)


PYPROJECT_TOML_SAMPLE_CONFIG = """\
[tool.dbt-bouncer]
dbt_artifacts_dir = "dbt_project/target"

[[tool.dbt-bouncer.manifest_checks]]
name = "check_model_has_unique_test"

[[tool.dbt-bouncer.manifest_checks]]
include = "^staging"
model_name_pattern = "^stg_"
name = "check_model_names"
"""

PYPROJECT_TOML_MISSPELLED_CONFIG = """\
[tool.dbt-bouncer-misspelled]
dbt_artifacts_dir = "dbt_project/target"
"""


def test_get_file_config_path_pyproject_toml(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    pyproject_file = tmp_path / "pyproject.toml"
    pyproject_file.write_text(PYPROJECT_TOML_SAMPLE_CONFIG)

    config_file_path = get_config_file_path(
        config_file="dbt_bouncer.yml",
        config_file_source="DEFAULT",
    )

    assert config_file_path == pyproject_file


def test_get_file_config_path_pyproject_toml_recursive(monkeypatch, tmp_path):
    Path.mkdir(tmp_path / "test")
    monkeypatch.chdir(tmp_path / "test")

    pyproject_file = tmp_path / "pyproject.toml"
    pyproject_file.write_text(PYPROJECT_TOML_SAMPLE_CONFIG)

    config_file_path = get_config_file_path(
        config_file="dbt_bouncer.yml",
        config_file_source="DEFAULT",
    )
    assert config_file_path == pyproject_file


def test_load_config_file_contents_create_default_config_file(
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)

    pyproject_file = tmp_path / "pyproject.toml"
    pyproject_file.write_text(PYPROJECT_TOML_MISSPELLED_CONFIG)

    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("CREATE_DBT_BOUNCER_CONFIG_FILE", "true")

        contents = load_config_file_contents(
            config_file_path=pyproject_file, allow_default_config_file_creation=True
        )
        assert list(contents.keys()) == [
            "manifest_checks",
        ]


def test_load_config_file_contents_pyproject_toml_no_bouncer_section(
    monkeypatch,
    tmp_path,
):
    monkeypatch.chdir(tmp_path)

    pyproject_file = tmp_path / "pyproject.toml"
    pyproject_file.write_text(PYPROJECT_TOML_MISSPELLED_CONFIG)

    with pytest.raises(RuntimeError):
        load_config_file_contents(
            config_file_path=tmp_path / "pyproject.toml",
            allow_default_config_file_creation=False,
        )


def test_validate_conf_target_default_value(monkeypatch):
    monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        bouncer_config = validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents={
                "manifest_checks": [
                    {"name": "check_model_has_unique_test"},
                    {"name": "check_exposure_based_on_view"},
                ]
            },
        )

    assert bouncer_config.dbt_artifacts_dir == "./target"


def test_validate_conf_target_env_var():
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx, pytest.MonkeyPatch.context() as mp:
        mp.setenv("DBT_PROJECT_DIR", "my_dbt_project_dir")
        bouncer_config = validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents={
                "manifest_checks": [
                    {"name": "check_model_has_unique_test"},
                    {"name": "check_exposure_based_on_view"},
                ]
            },
        )

    assert bouncer_config.dbt_artifacts_dir == "my_dbt_project_dir/target"


def test_validate_conf_target_override(monkeypatch):
    monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)
    ctx = click.Context(
        cli,
        obj={
            "config_file_path": "",
            "custom_checks_dir": None,
        },
    )

    with ctx:
        bouncer_config = validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents={
                "dbt_artifacts_dir": "somewhere_over_there/target",
                "manifest_checks": [
                    {"name": "check_model_has_unique_test"},
                    {"name": "check_exposure_based_on_view"},
                ],
            },
        )

    assert bouncer_config.dbt_artifacts_dir == "somewhere_over_there/target"


invalid_confs = [
    (
        f,
        pytest.raises(Exception),  # noqa: PT011
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
        result = validate_conf(
            check_categories=[x for x in conf if x.endswith("_checks")],
            config_file_contents=conf,
        )
        assert isinstance(result.exception, (RuntimeError, PydanticUserError))


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
            check_categories=["manifest_checks"],
            config_file_contents={
                "manifest_checks": [{"name": "check_model_has_unique_tst"}]
            },
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
            check_categories=["manifest_checks"],
            config_file_contents={
                "manifest_checks": [
                    {"name": "check_model_has_unique_tst"},
                    {"name": "check_exposure_based_on_viw"},
                ]
            },
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
        validate_conf(
            check_categories=[x for x in conf if x.endswith("_checks")],
            config_file_contents=conf,
        )


def test_lint_config_file_valid(tmp_path):
    """Test lint_config_file with a valid config."""
    from dbt_bouncer.config_file_validator import lint_config_file

    config = {
        "manifest_checks": [
            {"name": "check_model_description_populated"},
        ],
    }
    config_file = tmp_path / "dbt-bouncer.yml"
    with Path.open(config_file, "w") as f:
        yaml.dump(config, f)

    issues = lint_config_file(config_file)
    assert issues == []


def test_lint_config_file_missing_name(tmp_path):
    """Test lint_config_file with missing name field."""
    from dbt_bouncer.config_file_validator import lint_config_file

    config = {
        "manifest_checks": [
            {"description": "Missing name field"},
        ],
    }
    config_file = tmp_path / "dbt-bouncer.yml"
    with Path.open(config_file, "w") as f:
        yaml.dump(config, f)

    issues = lint_config_file(config_file)
    assert len(issues) == 1
    assert issues[0]["message"] == "Check is missing required 'name' field"
    assert issues[0]["severity"] == "error"


def test_lint_config_file_yaml_syntax_error(tmp_path):
    """Test lint_config_file with YAML syntax error."""
    from dbt_bouncer.config_file_validator import lint_config_file

    config_file = tmp_path / "dbt-bouncer.yml"
    with Path.open(config_file, "w") as f:
        f.write("manifest_checks:\n  - name: test\n    invalid yaml: [}")

    issues = lint_config_file(config_file)
    assert len(issues) == 1
    assert "YAML syntax error" in issues[0]["message"]
    assert issues[0]["severity"] == "error"


def test_lint_config_file_not_list(tmp_path):
    """Test lint_config_file when check category is not a list."""
    from dbt_bouncer.config_file_validator import lint_config_file

    config = {
        "manifest_checks": "not a list",
    }
    config_file = tmp_path / "dbt-bouncer.yml"
    with Path.open(config_file, "w") as f:
        yaml.dump(config, f)

    issues = lint_config_file(config_file)
    assert len(issues) == 1
    assert "must be a list" in issues[0]["message"]


def test_lint_config_file_empty(tmp_path):
    """Test lint_config_file with empty config."""
    from dbt_bouncer.config_file_validator import lint_config_file

    config_file = tmp_path / "dbt-bouncer.yml"
    config_file.write_text("")

    issues = lint_config_file(config_file)
    assert len(issues) == 1
    assert issues[0]["message"] == "Config file is empty"


def test_lint_config_file_not_yaml(tmp_path):
    """Test lint_config_file with non-YAML file."""
    from dbt_bouncer.config_file_validator import lint_config_file

    config_file = tmp_path / "dbt-bouncer.txt"
    config_file.write_text("not yaml")

    issues = lint_config_file(config_file)
    assert issues == []


def test_lint_config_file_multiple_issues(tmp_path):
    """Test lint_config_file with multiple issues."""
    from dbt_bouncer.config_file_validator import lint_config_file

    config = {
        "manifest_checks": [
            {"description": "Missing name 1"},
            {"name": "check_model_description_populated"},
            {"description": "Missing name 2"},
        ],
    }
    config_file = tmp_path / "dbt-bouncer.yml"
    with Path.open(config_file, "w") as f:
        yaml.dump(config, f)

    issues = lint_config_file(config_file)
    assert len(issues) == 2
