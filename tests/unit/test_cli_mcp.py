"""Unit tests for dbt_bouncer.cli.mcp."""

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from dbt_bouncer.cli.mcp import server as mcp_server
from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app

runner = CliRunner()


class TestListChecksTool:
    """Tests for the list_checks tool payload."""

    def test_all_categories(self):
        """Without a filter, all three categories are returned."""
        payload = mcp_server.list_checks()

        assert set(payload) == {
            "catalog_checks",
            "manifest_checks",
            "run_results_checks",
        }

    def test_category_filter(self):
        """A category filter returns only that category."""
        payload = mcp_server.list_checks("manifest_checks")

        assert list(payload) == ["manifest_checks"]
        assert len(payload["manifest_checks"]) > 0


class TestExplainCheckTool:
    """Tests for the explain_check tool payload."""

    def test_by_name(self):
        """A check is explained by its snake_case name."""
        payload = mcp_server.explain_check("check_model_names")

        assert payload["name"] == "check_model_names"
        assert payload["code"] == "MO038"
        assert payload["category"] == "manifest"
        assert "model_name_pattern" in payload["parameters"]
        assert "Example(s):" in payload["docstring"]
        # Delegating to the explain payload builder adds the docs URL.
        assert payload["documentation_url"] == (
            "https://godatadriven.github.io/dbt-bouncer/checks/manifest/models/naming/"
        )

    def test_by_rule_code(self):
        """A check is explained by its rule code."""
        payload = mcp_server.explain_check("MO038")

        assert payload["name"] == "check_model_names"

    def test_unknown_check(self):
        """An unknown check returns an error payload, not an exception."""
        payload = mcp_server.explain_check("check_model_namez")

        assert "error" in payload


class TestReadProjectConfigTool:
    """Tests for the read_project_config tool payload."""

    def test_valid_config(self, tmp_path, monkeypatch):
        """A valid config file is returned with valid=True."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "dbt-bouncer.yml").write_text(
            "manifest_checks:\n  - name: check_model_description_populated\n"
        )

        payload = mcp_server.read_project_config()

        assert payload["valid"] is True
        assert payload["errors"] == []
        assert payload["config"]["manifest_checks"][0] == {
            "name": "check_model_description_populated"
        }

    def test_invalid_config(self, tmp_path):
        """An invalid config file is returned with valid=False and errors."""
        config_file = tmp_path / "dbt-bouncer.yml"
        config_file.write_text("manifest_checks:\n  - name: check_model_namez\n")

        payload = mcp_server.read_project_config(str(config_file))

        assert payload["valid"] is False
        assert payload["errors"] != []

    def test_missing_config(self, tmp_path, monkeypatch):
        """A missing config file returns an error payload, not an exception."""
        monkeypatch.chdir(tmp_path)

        payload = mcp_server.read_project_config()

        assert payload["valid"] is False
        assert "error" in payload


class TestRunChecksTool:
    """Tests for the run_checks tool payload."""

    def test_executable_missing(self, monkeypatch):
        """A missing dbt-bouncer executable returns an error payload."""
        monkeypatch.setattr(mcp_server.shutil, "which", lambda _: None)

        payload = mcp_server.run_checks()

        assert "error" in payload

    def test_timeout_returns_error_payload(self, monkeypatch):
        """A hung subprocess surfaces as an error payload, not an exception."""

        def fake_run(cmd, **_kwargs):
            raise mcp_server.subprocess.TimeoutExpired(cmd=cmd, timeout=120)

        monkeypatch.setattr(mcp_server.shutil, "which", lambda _: "/usr/bin/fake")
        monkeypatch.setattr(mcp_server.subprocess, "run", fake_run)

        payload = mcp_server.run_checks()

        assert "did not finish" in payload["error"]

    def test_passing_run(self, monkeypatch):
        """A clean run maps to passed=True with no failures."""

        def fake_run(cmd, **_kwargs):
            output_file = Path(cmd[cmd.index("--output-file") + 1])
            output_file.write_text("[]")
            return SimpleNamespace(returncode=0, stdout="Done. SUCCESS=5\n", stderr="")

        monkeypatch.setattr(mcp_server.shutil, "which", lambda _: "/usr/bin/fake")
        monkeypatch.setattr(mcp_server.subprocess, "run", fake_run)

        payload = mcp_server.run_checks()

        assert payload["exit_code"] == 0
        assert payload["passed"] is True
        assert payload["failures"] == []

    def test_subprocess_invocation(self, monkeypatch):
        """The subprocess result and output file map into the payload."""

        def fake_run(cmd, **_kwargs):
            output_file = Path(cmd[cmd.index("--output-file") + 1])
            output_file.write_text('[{"check_run_id": "check_x:0:model_1"}]')
            assert "--config-file" in cmd
            assert "--only" in cmd
            assert "--check" in cmd
            return SimpleNamespace(returncode=1, stdout="Done. ERROR=1\n", stderr="")

        monkeypatch.setattr(mcp_server.shutil, "which", lambda _: "/usr/bin/fake")
        monkeypatch.setattr(mcp_server.subprocess, "run", fake_run)

        payload = mcp_server.run_checks(
            config_file="dbt-bouncer.yml",
            only="manifest_checks",
            check="check_x",
        )

        assert payload["exit_code"] == 1
        assert payload["passed"] is False
        assert payload["failures"] == [{"check_run_id": "check_x:0:model_1"}]
        assert "ERROR=1" in payload["console_output_tail"]


def test_build_server_registers_tools():
    """The FastMCP server registers all four tools."""
    pytest.importorskip("mcp")

    server = mcp_server.build_server()
    tools = asyncio.run(server.list_tools())

    assert sorted(t.name for t in tools) == [
        "explain_check",
        "list_checks",
        "read_project_config",
        "run_checks",
    ]


def test_mcp_command_without_dependency(monkeypatch):
    """The mcp command exits with CONFIG_ERROR when `mcp` is not installed."""

    def raise_import_error():
        raise ImportError("No module named 'mcp'")

    monkeypatch.setattr("dbt_bouncer.cli.mcp.server.build_server", raise_import_error)

    result = runner.invoke(app, ["mcp"])

    assert result.exit_code == ExitCode.CONFIG_ERROR
