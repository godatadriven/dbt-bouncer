"""Shared fixtures for the integration suite.

Integration tests drive the CLI (or `run_bouncer`) end to end against real dbt
artifacts, so almost every one needs a config file on disk pointing at a target
directory. These fixtures centralise that setup; before they existed the same
"dump YAML into tmp_path, copy a manifest next to it" block was inlined in
roughly eight tests.
"""

import json
import shutil
from collections.abc import Callable
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from tests.integration.constants import (
    DBT_112_TARGET,
    DBT_PROJECT_TARGET,
    EXAMPLE_CONFIG,
)


@pytest.fixture
def cli_runner() -> CliRunner:
    """Return a Typer `CliRunner`.

    Returns:
        CliRunner: A fresh runner for invoking the app.

    """
    return CliRunner()


@pytest.fixture
def write_config(tmp_path) -> Callable[..., Path]:
    """Return a factory writing an arbitrary bouncer config into `tmp_path`.

    Returns:
        Callable[..., Path]: Called with the config dict (and optionally a file
            name), returns the path it was written to.

    """

    def _write(config: dict, name: str = "dbt-bouncer.yml") -> Path:
        config_file = tmp_path / name
        with config_file.open("w", encoding="utf-8") as f:
            yaml.dump(config, f)
        return config_file

    return _write


@pytest.fixture
def write_checks_config(write_config) -> Callable[..., Path]:
    """Return a factory writing a config that runs `checks` against an artifacts dir.

    Returns:
        Callable[..., Path]: Called with a list of manifest checks and optionally
            an artifacts directory (default: the dbt_112 fixture), returns the
            config file path.

    """

    def _write(checks: list[dict], artifacts_dir: Path = DBT_112_TARGET) -> Path:
        return write_config(
            {"dbt_artifacts_dir": str(artifacts_dir), "manifest_checks": checks}
        )

    return _write


@pytest.fixture
def example_config() -> dict:
    """Return the shipped `dbt-bouncer-example.yml` parsed into a dict.

    Returns:
        dict: The example config, ready to be mutated and re-written by a test.

    """
    with EXAMPLE_CONFIG.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


@pytest.fixture
def artifacts_in_tmp_path(tmp_path) -> Path:
    """Copy the test project's manifest into `tmp_path`.

    Lets a config in the same directory use `dbt_artifacts_dir: "."`, which
    resolves relative to the config file.

    Returns:
        Path: The `tmp_path` now holding a `manifest.json`.

    """
    shutil.copy(DBT_PROJECT_TARGET / "manifest.json", tmp_path / "manifest.json")
    return tmp_path


@pytest.fixture
def artifacts_with_skip_checks(tmp_path) -> Callable[..., Path]:
    """Return a factory writing an artifacts dir whose models carry `skip_checks` meta.

    `skip_checks` is read off a resource's own dbt meta, so a test that covers
    it end to end needs a manifest that carries it. The committed fixtures are
    generated from `dbt_project`, so the factory derives a copy in `tmp_path`
    rather than edit them.

    Returns:
        Callable[..., Path]: Called with a mapping of model name to the
            `skip_checks` entries that model must carry (and optionally the
            artifacts directory to derive from), returns the new directory.

    """

    def _write(
        skip_checks_by_model: dict[str, list[str]],
        artifacts_dir: Path = DBT_112_TARGET,
    ) -> Path:
        with (artifacts_dir / "manifest.json").open("r", encoding="utf-8") as f:
            manifest = json.load(f)

        patched = set()
        for node in manifest["nodes"].values():
            name = node.get("name")
            if node.get("resource_type") != "model" or name not in skip_checks_by_model:
                continue
            meta = node.setdefault("config", {}).setdefault("meta", {})
            meta["dbt-bouncer"] = {"skip_checks": skip_checks_by_model[name]}
            patched.add(name)

        # A silent miss would leave the test asserting against an unmodified
        # manifest, which passes for the wrong reason.
        missing = sorted(set(skip_checks_by_model) - patched)
        if missing:
            msg = f"No model named {missing} in `{artifacts_dir}`."
            raise ValueError(msg)

        out_dir = tmp_path / "artifacts_with_skip_checks"
        out_dir.mkdir(exist_ok=True)
        with (out_dir / "manifest.json").open("w", encoding="utf-8") as f:
            json.dump(manifest, f)
        return out_dir

    return _write
