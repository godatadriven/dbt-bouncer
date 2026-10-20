"""Unit tests for dbt_bouncer.cli.baseline."""

import importlib
import logging
from pathlib import PurePath

import orjson
import pytest
from typer.testing import CliRunner

from dbt_bouncer.cli.baseline.utils import DEFAULT_BASELINE_FILE, write_baseline
from dbt_bouncer.enums import CheckOutcome, ExitCode
from dbt_bouncer.exceptions import DbtBouncerArtifactError, DbtBouncerConfigError
from dbt_bouncer.main import app

# Import the module explicitly: the package attribute `baseline` would resolve
# to the command function rather than the module that holds `write_baseline`.
baseline_module = importlib.import_module("dbt_bouncer.cli.baseline")

runner = CliRunner()


def test_baseline_records_current_failures(failing_config_file, tmp_path):
    """The command writes every current failure to the baseline file."""
    baseline_file = tmp_path / "baseline.json"

    result = runner.invoke(
        app,
        [
            "baseline",
            "--config-file",
            PurePath(failing_config_file).as_posix(),
            "--output-file",
            PurePath(baseline_file).as_posix(),
        ],
        env={"COLUMNS": "400"},
    )

    assert result.exit_code == ExitCode.SUCCESS, result.output
    document = orjson.loads(baseline_file.read_bytes())
    assert document["version"] == 1
    assert len(document["failures"]) > 0
    assert {e["check_name"] for e in document["failures"]} == {"check_model_names"}
    assert f"Wrote {len(document['failures'])} failure(s)" in result.output


def test_write_baseline_excludes_internal_errors(
    caplog, failing_config_file, monkeypatch, tmp_path
):
    """Internal errors are left out of the baseline and reported as a warning."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "dbt_bouncer.runner.collect_failures",
        lambda _ctx: [
            {
                "check_run_id": "check_model_names:0:model.a",
                "outcome": CheckOutcome.FAILED,
                "unique_id": "model.a",
            },
            {
                "check_run_id": "check_model_names:0:model.b",
                "outcome": CheckOutcome.INTERNAL_ERROR,
                "unique_id": "model.b",
            },
        ],
    )

    with caplog.at_level(logging.WARNING):
        count = write_baseline(config_file=failing_config_file)

    assert count == 1
    assert "1 check(s) raised an unexpected error" in caplog.text
    # Without `output_file`, the baseline goes to the default path.
    document = orjson.loads((tmp_path / DEFAULT_BASELINE_FILE).read_bytes())
    assert [e["unique_id"] for e in document["failures"]] == ["model.a"]


@pytest.mark.parametrize(
    ("error", "exit_code"),
    [
        pytest.param(
            DbtBouncerArtifactError("manifest.json is missing"),
            ExitCode.ARTIFACT_ERROR,
            id="artifact_error",
        ),
        pytest.param(
            DbtBouncerConfigError("config is invalid"),
            ExitCode.CONFIG_ERROR,
            id="config_error",
        ),
    ],
)
def test_baseline_maps_errors_to_exit_codes(caplog, error, exit_code, monkeypatch):
    """Typed errors from writing the baseline map to their exit codes."""

    def raise_error(**_kwargs):
        raise error

    monkeypatch.setattr(baseline_module, "write_baseline", raise_error)

    with caplog.at_level(logging.ERROR):
        result = runner.invoke(app, ["baseline"])

    assert result.exit_code == exit_code
    assert str(error) in caplog.text
