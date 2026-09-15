"""Integration tests for the default `run` command against real dbt artifacts."""

import json
import re
from pathlib import PurePath

import pytest

from dbt_bouncer.main import app
from tests.integration.constants import ARTIFACT_DIRS, ARTIFACT_IDS

_FAILING_DIRECTORIES_CHECK = {
    "name": "check_model_directories",
    "include": "",
    "permitted_sub_directories": ["staging"],
}

_FAILURE_BANNER = (
    "`dbt-bouncer` failed. Please see below for more details or run "
    "`dbt-bouncer` with the `-v` flag."
)


@pytest.mark.parametrize("artifacts_dir", ARTIFACT_DIRS, ids=ARTIFACT_IDS)
def test_run_happy_path_across_artifact_versions(
    artifacts_dir, caplog, cli_runner, example_config, write_config
):
    """The shipped example config passes against every supported artifact format."""
    example_config["dbt_artifacts_dir"] = str(artifacts_dir / "target")
    config_file = write_config(example_config, name="dbt-bouncer-example.yml")

    result = cli_runner.invoke(app, f"--config-file {PurePath(config_file).as_posix()}")

    assert "Running dbt-bouncer (0.0.0)..." in caplog.text
    # The artifact summary table is printed once and holds at least one non-zero
    # count, guarding against a parser regression that silently produces empty
    # artifact lists. It goes through Rich's Console rather than the logger, so
    # it is asserted against result.output. Rich falls back to ASCII separators
    # on legacy Windows consoles, hence accepting either "│" or "|".
    assert result.output.count("manifest.json") == 1
    assert re.search(r"[│|]\s+[1-9]", result.output), (
        "Artifact summary table contains no non-zero counts"
    )
    assert result.exit_code == 0


def test_run_writes_coverage_file(
    artifacts_in_tmp_path, caplog, cli_runner, write_config
):
    """`--output-file` writes a coverage document alongside the failure exit code."""
    config_file = write_config(
        {"dbt_artifacts_dir": ".", "manifest_checks": [_FAILING_DIRECTORIES_CHECK]}
    )
    coverage_file = artifacts_in_tmp_path / "coverage.json"

    result = cli_runner.invoke(
        app,
        [
            "--config-file",
            str(config_file),
            "--output-file",
            str(coverage_file),
        ],
    )

    assert coverage_file.exists()
    with coverage_file.open("r", encoding="utf-8") as f:
        coverage = json.load(f)
    assert len(coverage) > 1
    assert f"Saving coverage file to `{coverage_file}`".replace(
        "\\", "/"
    ) in caplog.text.replace("\\", "/")
    assert result.exit_code == 1


@pytest.mark.usefixtures("artifacts_in_tmp_path")
def test_run_reports_failure_message(caplog, cli_runner, write_config):
    """A failing check logs the summary banner pointing at `-v` for detail."""
    config_file = write_config(
        {"dbt_artifacts_dir": ".", "manifest_checks": [_FAILING_DIRECTORIES_CHECK]}
    )

    result = cli_runner.invoke(app, ["--config-file", str(config_file)])

    assert _FAILURE_BANNER in caplog.text
    assert result.exit_code == 1
