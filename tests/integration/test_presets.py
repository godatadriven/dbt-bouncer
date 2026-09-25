"""Integration tests for `run --preset`, against the dbt_112 fixture.

A preset carries no `dbt_artifacts_dir`, so it reads `$DBT_PROJECT_DIR/target`.
Each test points that at the fixture and moves into an empty `tmp_path`, so no
config file on disk is involved.
"""

import json
from pathlib import PurePath

import pytest

from dbt_bouncer.enums import ExitCode, PresetName
from dbt_bouncer.main import app
from dbt_bouncer.presets import load_preset_contents
from tests.integration.constants import DBT_112_TARGET, strip_ansi


@pytest.fixture
def _preset_env(monkeypatch, tmp_path):
    monkeypatch.setenv("DBT_PROJECT_DIR", str(DBT_112_TARGET.parent))
    monkeypatch.chdir(tmp_path)


@pytest.mark.usefixtures("_preset_env")
@pytest.mark.parametrize("preset", list(PresetName), ids=[p.value for p in PresetName])
def test_preset_runs_every_check_it_configures(caplog, cli_runner, preset, tmp_path):
    """Every check a preset names is valid and matches at least one resource.

    This is a smoke test for the presets as checks change: a renamed check or a
    changed parameter fails validation, and a check that stops matching anything
    goes missing from the output file.
    """
    output_file = tmp_path / "results.json"

    result = cli_runner.invoke(
        app, ["run", "--preset", preset.value, "--output-file", str(output_file)]
    )

    # Whether the fixture passes a preset is not the point, only that it ran.
    assert result.exit_code in {ExitCode.SUCCESS, ExitCode.CHECK_ERRORS}, result.output
    assert f"Using the `{preset.value}` preset configuration." in caplog.text
    configured = {
        c["name"] for checks in load_preset_contents(preset).values() for c in checks
    }
    results = json.loads(output_file.read_text(encoding="utf-8"))
    assert {r["check_run_id"].split(":")[0] for r in results} == configured


@pytest.mark.usefixtures("_preset_env")
def test_preset_is_case_insensitive(cli_runner):
    """`--preset MINIMAL` resolves to the `minimal` preset."""
    result = cli_runner.invoke(app, ["run", "--preset", "MINIMAL"])

    assert result.exit_code in {ExitCode.SUCCESS, ExitCode.CHECK_ERRORS}, result.output


@pytest.mark.usefixtures("_preset_env")
def test_unknown_preset_is_rejected(cli_runner):
    """An unknown preset name is a usage error, before any artifact is read."""
    result = cli_runner.invoke(app, ["run", "--preset", "lenient"])

    assert result.exit_code == 2
    assert "lenient" in strip_ansi(result.output)


@pytest.mark.usefixtures("_preset_env")
def test_explicit_config_file_overrides_preset(caplog, cli_runner, write_checks_config):
    """With both flags, the config file runs and `--preset` is ignored with a warning."""
    config_file = write_checks_config(
        [
            {
                "name": "check_model_names",
                "include": r"models/staging/crm/stg_orders\.sql",
                "model_name_pattern": "^stg_",
            }
        ]
    )

    result = cli_runner.invoke(
        app,
        [
            "run",
            "--preset",
            "strict",
            "--config-file",
            PurePath(config_file).as_posix(),
        ],
    )

    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert "Ignoring `--preset strict` and using the config file." in caplog.text
    assert "Using the `strict` preset configuration." not in caplog.text
    # Only the single configured check ran, not the dozens in `strict`.
    assert "SUCCESS=1 WARN=0 ERROR=0" in strip_ansi(result.output)
