import os
import tomllib
from contextlib import nullcontext as does_not_raise
from pathlib import Path
from typing import Any
from unittest import mock

import pytest
import typer
import yaml
from pydantic import PydanticUserError
from typer.main import get_command

from dbt_bouncer.configuration_file.validator import (
    _get_stub_namespace,
    get_config_file_path,
    load_config_file_contents,
    validate_conf,
)
from dbt_bouncer.enums import ConfigFileSource
from dbt_bouncer.main import app


def test_get_file_config_path_commandline(tmp_path):
    config_file = tmp_path / "my_dbt_bouncer.yml"
    config_file.write_text("test: 1")
    config_file_path = get_config_file_path(
        config_file=str(config_file),
        config_file_source=ConfigFileSource.COMMANDLINE,
    )

    assert config_file_path.replace("\\", "/") == config_file.as_posix().replace(
        "\\", "/"
    )


def test_get_file_config_path_default(tmp_path):
    config_file = tmp_path / "dbt_bouncer.yml"
    config_file.write_text("test: 1")
    config_file_path = get_config_file_path(
        config_file=str(config_file),
        config_file_source=ConfigFileSource.DEFAULT,
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
            config_file_source=ConfigFileSource.DEFAULT,
        )

    assert config_file_path == Path(custom_config_file_path)


DBT_BOUNCER_TOML_SAMPLE_CONFIG = """\
dbt_artifacts_dir = "dbt_project/target"

[[manifest_checks]]
name = "check_model_has_unique_test"

[[manifest_checks]]
include = "^staging"
model_name_pattern = "^stg_"
name = "check_model_names"
"""

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
        config_file_source=ConfigFileSource.DEFAULT,
    )

    assert config_file_path == pyproject_file


def test_get_file_config_path_pyproject_toml_recursive(monkeypatch, tmp_path):
    Path.mkdir(tmp_path / "test")
    monkeypatch.chdir(tmp_path / "test")

    pyproject_file = tmp_path / "pyproject.toml"
    pyproject_file.write_text(PYPROJECT_TOML_SAMPLE_CONFIG)

    config_file_path = get_config_file_path(
        config_file="dbt_bouncer.yml",
        config_file_source=ConfigFileSource.DEFAULT,
    )
    assert config_file_path == pyproject_file


def test_get_file_config_path_dbt_bouncer_toml(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    toml_file = tmp_path / "dbt-bouncer.toml"
    toml_file.write_text(DBT_BOUNCER_TOML_SAMPLE_CONFIG)

    config_file_path = get_config_file_path(
        config_file="dbt_bouncer.yml",
        config_file_source=ConfigFileSource.DEFAULT,
    )

    assert config_file_path == toml_file


def test_get_file_config_path_yml_preferred_over_toml(monkeypatch, tmp_path):
    """dbt-bouncer.yml should take priority over dbt-bouncer.toml."""
    monkeypatch.chdir(tmp_path)

    yml_file = tmp_path / "dbt-bouncer.yml"
    yml_file.write_text("manifest_checks: []")
    toml_file = tmp_path / "dbt-bouncer.toml"
    toml_file.write_text(DBT_BOUNCER_TOML_SAMPLE_CONFIG)

    config_file_path = get_config_file_path(
        config_file=str(yml_file),
        config_file_source=ConfigFileSource.DEFAULT,
    )

    assert config_file_path == yml_file


def test_load_config_file_contents_dbt_bouncer_toml(tmp_path):
    toml_file = tmp_path / "dbt-bouncer.toml"
    toml_file.write_text(DBT_BOUNCER_TOML_SAMPLE_CONFIG)

    contents = load_config_file_contents(config_file_path=toml_file)

    assert contents["dbt_artifacts_dir"] == "dbt_project/target"
    assert len(contents["manifest_checks"]) == 2
    assert contents["manifest_checks"][0]["name"] == "check_model_has_unique_test"


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
    ctx = typer.Context(
        get_command(app),
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
    ctx = typer.Context(
        get_command(app),
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
    ctx = typer.Context(
        get_command(app),
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

    ctx = typer.Context(
        get_command(app),
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
    ctx = typer.Context(
        get_command(app),
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
    ctx = typer.Context(
        get_command(app),
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

    ctx = typer.Context(
        get_command(app),
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


valid_toml_confs = [
    (
        f,
        does_not_raise(),
    )
    for f in Path("./tests/unit/config_files/valid").glob("*.toml")
]


@pytest.mark.parametrize(
    ("f", "expectation"),
    valid_toml_confs,
    ids=[f.stem for f, _ in valid_toml_confs],
)
def test_validate_conf_valid_toml(f, expectation):
    with Path.open(f, "rb") as fp:
        conf = tomllib.load(fp)

    ctx = typer.Context(
        get_command(app),
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
    from dbt_bouncer.configuration_file.validator import lint_config_file

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
    from dbt_bouncer.configuration_file.validator import lint_config_file

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
    from dbt_bouncer.configuration_file.validator import lint_config_file

    config_file = tmp_path / "dbt-bouncer.yml"
    with Path.open(config_file, "w") as f:
        f.write("manifest_checks:\n  - name: test\n    invalid yaml: [}")

    issues = lint_config_file(config_file)
    assert len(issues) == 1
    assert "YAML syntax error" in issues[0]["message"]
    assert issues[0]["severity"] == "error"


def test_lint_config_file_unexpected_exception(monkeypatch, tmp_path):
    """Test lint_config_file logs and reports unexpected exceptions instead of swallowing them."""
    from dbt_bouncer.configuration_file.validator import lint_config_file

    config_file = tmp_path / "dbt-bouncer.yml"
    config_file.write_text("manifest_checks:\n  - name: check_model_has_description\n")

    monkeypatch.setattr(
        Path,
        "read_text",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            PermissionError("access denied")
        ),
    )

    issues = lint_config_file(config_file)
    assert len(issues) == 1
    assert "Unexpected error during config parsing" in issues[0]["message"]
    assert "access denied" in issues[0]["message"]
    assert issues[0]["severity"] == "error"


def test_lint_config_file_not_list(tmp_path):
    """Test lint_config_file when check category is not a list."""
    from dbt_bouncer.configuration_file.validator import lint_config_file

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
    from dbt_bouncer.configuration_file.validator import lint_config_file

    config_file = tmp_path / "dbt-bouncer.yml"
    config_file.write_text("")

    issues = lint_config_file(config_file)
    assert len(issues) == 1
    assert issues[0]["message"] == "Config file is empty"


def test_lint_config_file_not_yaml(tmp_path):
    """Test lint_config_file with non-YAML file."""
    from dbt_bouncer.configuration_file.validator import lint_config_file

    config_file = tmp_path / "dbt-bouncer.txt"
    config_file.write_text("not yaml")

    issues = lint_config_file(config_file)
    assert issues == []


def test_lint_config_file_multiple_issues(tmp_path):
    """Test lint_config_file with multiple issues."""
    from dbt_bouncer.configuration_file.validator import lint_config_file

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


def test_get_stub_namespace_nested_dict_is_real_class():
    """NestedDict must be the real class, not Any — it is used in config fields."""
    from dbt_bouncer.check_framework.exceptions import NestedDict

    ns = _get_stub_namespace()
    assert ns["NestedDict"] is NestedDict


def test_get_stub_namespace_artifact_types_are_any():
    """All types except NestedDict should be Any stubs."""
    ns = _get_stub_namespace()
    for key, value in ns.items():
        if key != "NestedDict":
            assert value is Any, f"{key} should be Any but is {value}"


def test_validate_conf_with_stub_namespace(monkeypatch):
    """Config validation must succeed using stub types (no real artifact imports needed)."""
    monkeypatch.delenv("DBT_PROJECT_DIR", raising=False)
    ctx = typer.Context(
        get_command(app),
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


@pytest.fixture
def isolated_cache_dir(tmp_path, monkeypatch):
    """Point the validated-conf cache at a temp directory for the test.

    Returns:
        Path: The temp directory used as the cache root.

    """
    import dbt_bouncer.utils as utils_mod

    monkeypatch.setattr(utils_mod, "get_cache_dir", lambda: tmp_path)
    monkeypatch.delenv("DBT_BOUNCER_DISABLE_CONF_CACHE", raising=False)
    return tmp_path


def test_validate_conf_writes_cache_file_on_cold_path(isolated_cache_dir):
    """First validate_conf call writes a JSON cache file for the resolved conf."""
    ctx = typer.Context(
        get_command(app),
        obj={"config_file_path": "", "custom_checks_dir": None},
    )
    config = {"manifest_checks": [{"name": "check_model_has_unique_test"}]}
    with ctx:
        validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents=config,
        )

    assert len(list(isolated_cache_dir.glob("conf_*.json"))) == 1


def test_validate_conf_warm_path_matches_cold(isolated_cache_dir):  # noqa: ARG001
    """A warm validate_conf returns a config equivalent to the cold-path one."""
    config = {
        "manifest_checks": [
            {"name": "check_model_has_unique_test"},
            {"name": "check_exposure_based_on_view"},
        ]
    }
    ctx = typer.Context(
        get_command(app),
        obj={"config_file_path": "", "custom_checks_dir": None},
    )
    with ctx:
        cold = validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents=dict(config),
        )
        warm = validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents=dict(config),
        )

    assert [type(c).__qualname__ for c in warm.manifest_checks] == [
        type(c).__qualname__ for c in cold.manifest_checks
    ]
    assert [c.name for c in warm.manifest_checks] == [
        c.name for c in cold.manifest_checks
    ]
    assert warm.dbt_artifacts_dir == cold.dbt_artifacts_dir


def test_validate_conf_warm_path_rehydrates_nested_dict_field(isolated_cache_dir):  # noqa: ARG001
    """Warm-cache load must re-coerce RootModel fields (e.g. NestedDict).

    Regression: a previous implementation used ``model_construct`` on the warm
    path, which left ``keys`` as a raw ``list`` after JSON round-tripping and
    broke any check that called ``.model_dump()`` on it.
    """
    from dbt_bouncer.check_framework.exceptions import NestedDict

    config = {
        "manifest_checks": [
            {"name": "check_model_has_meta_keys", "keys": ["maturity", "owner"]},
        ]
    }
    ctx = typer.Context(
        get_command(app),
        obj={"config_file_path": "", "custom_checks_dir": None},
    )
    with ctx:
        validate_conf(  # cold path: populates the cache
            check_categories=["manifest_checks"],
            config_file_contents=dict(config),
        )
        warm = validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents=dict(config),
        )

    keys = warm.manifest_checks[0].keys
    assert isinstance(keys, NestedDict)
    assert keys.model_dump() == ["maturity", "owner"]


def test_validate_conf_disable_env_var_skips_cache(isolated_cache_dir, monkeypatch):
    """DBT_BOUNCER_DISABLE_CONF_CACHE=1 must skip both read and write paths."""
    monkeypatch.setenv("DBT_BOUNCER_DISABLE_CONF_CACHE", "1")
    ctx = typer.Context(
        get_command(app),
        obj={"config_file_path": "", "custom_checks_dir": None},
    )
    with ctx:
        validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents={
                "manifest_checks": [{"name": "check_model_has_unique_test"}]
            },
        )

    assert list(isolated_cache_dir.glob("conf_*.json")) == []


def test_validate_conf_invalid_cache_falls_back_to_cold_path(isolated_cache_dir):
    """A corrupt or wrong-version cache file is ignored, not raised."""
    import orjson

    from dbt_bouncer.utils import compute_conf_cache_key
    from dbt_bouncer.version import version

    config = {"manifest_checks": [{"name": "check_model_has_unique_test"}]}
    ver = version()
    key = compute_conf_cache_key(ver, config, ["manifest_checks"])
    cache_file = isolated_cache_dir / f"conf_{ver}_{key}.json"
    cache_file.write_bytes(orjson.dumps({"v": 999, "garbage": True}))

    ctx = typer.Context(
        get_command(app),
        obj={"config_file_path": "", "custom_checks_dir": None},
    )
    with ctx:
        bouncer_config = validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents=dict(config),
        )

    assert len(bouncer_config.manifest_checks) == 1
    assert bouncer_config.manifest_checks[0].name == "check_model_has_unique_test"


CUSTOM_CHECK_SOURCE = '''\
"""Trivial custom check used to exercise the conf cache + custom_checks_dir path."""

from typing import Literal

from dbt_bouncer.check_framework.base import BaseCheck


class CheckCustomNoop(BaseCheck):
    """No-op check for tests."""

    name: Literal["check_custom_noop"]
    model: object = None

    def execute(self) -> None:  # pragma: no cover - never executed in cache tests
        """Do nothing."""
'''


def _write_custom_checks_dir(root: "Path") -> "Path":
    """Materialise a minimal valid custom checks directory under ``root``.

    Returns:
        Path: The directory containing the custom check.

    """
    custom_dir = root / "custom_checks"
    (custom_dir / "manifest").mkdir(parents=True, exist_ok=True)
    (custom_dir / "manifest" / "check_custom_noop.py").write_text(CUSTOM_CHECK_SOURCE)
    return custom_dir


def test_validate_conf_warm_path_with_custom_checks_dir(isolated_cache_dir, tmp_path):  # noqa: ARG001
    """Cold + warm runs against a custom_checks_dir must agree.

    Exercises both ``compute_conf_cache_key``'s custom-dir mtime hashing and
    the cached-class lookup for non-``dbt_bouncer.checks.*`` modules.
    """
    custom_dir = _write_custom_checks_dir(tmp_path)
    config = {"manifest_checks": [{"name": "check_custom_noop"}]}

    ctx = typer.Context(
        get_command(app),
        obj={"config_file_path": "", "custom_checks_dir": str(custom_dir)},
    )
    with ctx:
        cold = validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents=dict(config),
            custom_checks_dir=custom_dir,
        )
        warm = validate_conf(
            check_categories=["manifest_checks"],
            config_file_contents=dict(config),
            custom_checks_dir=custom_dir,
        )

    assert len(cold.manifest_checks) == 1
    assert len(warm.manifest_checks) == 1
    assert (
        type(warm.manifest_checks[0]).__qualname__
        == type(cold.manifest_checks[0]).__qualname__
        == "CheckCustomNoop"
    )
    assert warm.manifest_checks[0].name == cold.manifest_checks[0].name


def test_compute_conf_cache_key_includes_custom_checks_dir(tmp_path):
    """Editing a file under custom_checks_dir must invalidate the conf cache key."""
    import os
    import time

    from dbt_bouncer.utils import compute_conf_cache_key

    custom_dir = _write_custom_checks_dir(tmp_path)
    config = {"manifest_checks": [{"name": "check_custom_noop"}]}

    key_before = compute_conf_cache_key(
        "0.0.0", config, ["manifest_checks"], custom_checks_dir=custom_dir
    )
    target = custom_dir / "manifest" / "check_custom_noop.py"
    os.utime(target, (time.time(), time.time() + 5))
    key_after = compute_conf_cache_key(
        "0.0.0", config, ["manifest_checks"], custom_checks_dir=custom_dir
    )

    assert key_before != key_after
