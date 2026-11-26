"""Unit tests for dbt_bouncer.cli.mcp."""

import asyncio
import os
import shutil
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from typer.testing import CliRunner

from dbt_bouncer.cli.mcp import server as mcp_server
from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app

runner = CliRunner()


@pytest.fixture(autouse=True)
def _reset_mcp_artifact_cache():
    """Keep the in-memory MCP artifact cache isolated per test."""
    mcp_server.clear_artifact_cache()
    yield
    mcp_server.clear_artifact_cache()


class TestListChecksTool:
    """Tests for the list_checks tool payload."""

    def test_all_categories(self):
        """Without a filter, every category is returned."""
        payload = mcp_server.list_checks()

        assert set(payload) == {
            "catalog_checks",
            "info_schema_checks",
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

    def test_custom_checks_dir_resolved_relative_to_config(self, tmp_path):
        """A configured `custom_checks_dir` is resolved next to the config file."""
        (tmp_path / "custom_checks").mkdir()
        config_file = tmp_path / "dbt-bouncer.yml"
        config_file.write_text(
            "custom_checks_dir: custom_checks\n"
            "manifest_checks:\n  - name: check_model_description_populated\n"
        )

        payload = mcp_server.read_project_config(str(config_file))

        assert payload["valid"] is True
        assert payload["config"]["custom_checks_dir"] == "custom_checks"


def _write_test_project_config(
    tmp_path: Path,
    artifacts_dir: Path,
    body: str,
    filename: str = "dbt-bouncer.yml",
) -> Path:
    cfg = tmp_path / filename
    cfg.write_text(f"dbt_artifacts_dir: {artifacts_dir.as_posix()}\n{body}")
    return cfg


class TestRunChecksTool:
    """Tests for the in-process cached run_checks tool payload."""

    def test_passing_run_and_stdout_isolation(self, tmp_path, capsys):
        """A clean in-process run passes and writes nothing to real stdout."""
        artifacts_dir = Path("dbt_project/target").resolve()
        cfg = _write_test_project_config(
            tmp_path,
            artifacts_dir,
            "manifest_checks:\n  - name: check_model_description_populated\n",
        )

        payload = mcp_server.run_checks(config_file=str(cfg))

        captured = capsys.readouterr()
        assert captured.out == ""
        assert payload["exit_code"] == 0
        assert payload["passed"] is True
        assert payload["failures"] == []
        assert payload["console_output_tail"] != ""

    def test_failing_run_with_filters(self, tmp_path):
        """Failing checks surface in `failures` and honour `only` and `check`."""
        artifacts_dir = Path("dbt_project/target").resolve()
        cfg = _write_test_project_config(
            tmp_path,
            artifacts_dir,
            (
                "catalog_checks:\n"
                "  - name: check_column_names\n"
                "    column_name_pattern: '^[a-z_0-9]*$'\n"
                "manifest_checks:\n"
                "  - name: check_model_names\n"
                "    model_name_pattern: '^never_matches_anything$'\n"
                "  - name: check_model_description_populated\n"
            ),
        )

        payload = mcp_server.run_checks(
            config_file=str(cfg),
            only="manifest_checks",
            check="check_model_names",
        )

        assert payload["exit_code"] == int(ExitCode.CHECK_ERRORS)
        assert payload["passed"] is False
        assert len(payload["failures"]) > 0
        assert all(
            f["check_run_id"].startswith("check_model_names:")
            for f in payload["failures"]
        )

    def test_default_config_resolution(self, tmp_path, monkeypatch):
        """Omitting config_file resolves dbt-bouncer.yml in the working directory."""
        artifacts_dir = Path("dbt_project/target").resolve()
        _write_test_project_config(
            tmp_path,
            artifacts_dir,
            "manifest_checks:\n  - name: check_model_description_populated\n",
        )
        monkeypatch.chdir(tmp_path)

        payload = mcp_server.run_checks()

        assert payload["exit_code"] == 0
        assert payload["passed"] is True

    def test_config_error_returns_config_error_exit_code(self, tmp_path):
        """A missing config file maps to CONFIG_ERROR without raising."""
        payload = mcp_server.run_checks(config_file=str(tmp_path / "missing.yml"))

        assert payload["exit_code"] == int(ExitCode.CONFIG_ERROR)
        assert payload["passed"] is False
        assert payload["failures"] == []
        assert "not found" in payload["console_output_tail"]

    def test_artifact_error_returns_artifact_error_exit_code(self, tmp_path):
        """Missing artifacts map to ARTIFACT_ERROR without raising."""
        empty_target = tmp_path / "empty_target"
        empty_target.mkdir()
        cfg = _write_test_project_config(
            tmp_path,
            empty_target,
            "manifest_checks:\n  - name: check_model_description_populated\n",
        )

        payload = mcp_server.run_checks(config_file=str(cfg))

        assert payload["exit_code"] == int(ExitCode.ARTIFACT_ERROR)
        assert payload["passed"] is False
        assert payload["failures"] == []
        assert "manifest.json" in payload["console_output_tail"]

    def test_caches_parsed_artifacts_and_invalidates_on_mtime_change(
        self, tmp_path, monkeypatch
    ):
        """Repeated calls reuse parsed artifacts until an artifact file changes."""
        import dbt_bouncer.artifact_parsers.parser as parser_mod

        target = tmp_path / "target"
        target.mkdir()
        shutil.copy(Path("dbt_project/target/manifest.json"), target / "manifest.json")
        shutil.copy(Path("dbt_project/target/catalog.json"), target / "catalog.json")
        shutil.copy(
            Path("dbt_project/target/run_results.json"), target / "run_results.json"
        )
        shutil.copytree(Path("dbt_project/target/info_schema"), target / "info_schema")

        cfg = _write_test_project_config(
            tmp_path,
            target,
            (
                "catalog_checks:\n"
                "  - name: check_column_names\n"
                "    column_name_pattern: '^[a-z_0-9]*$'\n"
                "info_schema_checks:\n"
                "  - name: check_info_schema_query\n"
                "    sql: 'SELECT 1 WHERE 1 = 0'\n"
                "manifest_checks:\n"
                "  - name: check_model_description_populated\n"
                "run_results_checks:\n"
                "  - name: check_run_results_max_execution_time\n"
                "    max_execution_time_seconds: 60\n"
            ),
        )

        parse_calls = 0
        real_parse = parser_mod.parse_dbt_artifacts

        def counting_parse(*args, **kwargs):
            nonlocal parse_calls
            parse_calls += 1
            return real_parse(*args, **kwargs)

        monkeypatch.setattr(parser_mod, "parse_dbt_artifacts", counting_parse)

        # First call parses all configured artifacts.
        first = mcp_server.run_checks(config_file=str(cfg))
        assert first["passed"] is True
        assert parse_calls == 1

        # Second identical call hits the in-memory cache.
        second = mcp_server.run_checks(config_file=str(cfg))
        assert second["passed"] is True
        assert parse_calls == 1

        # Subset category call still hits the cache.
        subset = mcp_server.run_checks(config_file=str(cfg), only="manifest_checks")
        assert subset["passed"] is True
        assert parse_calls == 1

        # Bumping manifest.json mtime invalidates the cache.
        manifest_path = target / "manifest.json"
        st = manifest_path.stat()
        os.utime(manifest_path, ns=(st.st_atime_ns, st.st_mtime_ns + 1_000_000))

        third = mcp_server.run_checks(config_file=str(cfg))
        assert third["passed"] is True
        assert parse_calls == 2

    def test_cache_upgrades_and_evicts_oldest_entry(self, tmp_path, monkeypatch):
        """Requesting new artifact types upgrades the entry; exceeding max size evicts oldest."""
        import dbt_bouncer.artifact_parsers.parser as parser_mod

        real_parse = parser_mod.parse_dbt_artifacts
        parse_calls = 0

        def counting_parse(*args, **kwargs):
            nonlocal parse_calls
            parse_calls += 1
            return real_parse(*args, **kwargs)

        monkeypatch.setattr(parser_mod, "parse_dbt_artifacts", counting_parse)

        # Create 2 distinct targets and lower the cache cap to 1 to test eviction.
        monkeypatch.setattr(mcp_server, "_MAX_ARTIFACT_CACHE_ENTRIES", 1)
        t1 = tmp_path / "t1"
        t2 = tmp_path / "t2"
        for t in (t1, t2):
            t.mkdir()
            shutil.copy(Path("dbt_project/target/manifest.json"), t / "manifest.json")
            shutil.copy(Path("dbt_project/target/catalog.json"), t / "catalog.json")

        cfg1_manifest = _write_test_project_config(
            tmp_path,
            t1,
            "manifest_checks:\n  - name: check_model_description_populated\n",
            filename="c1_m.yml",
        )
        cfg1_catalog = _write_test_project_config(
            tmp_path,
            t1,
            (
                "catalog_checks:\n"
                "  - name: check_column_names\n"
                "    column_name_pattern: '^[a-z_0-9]*$'\n"
            ),
            filename="c1_c.yml",
        )
        cfg2_manifest = _write_test_project_config(
            tmp_path,
            t2,
            "manifest_checks:\n  - name: check_model_description_populated\n",
            filename="c2_m.yml",
        )

        # Populate cache with t1 (manifest only).
        assert mcp_server.run_checks(config_file=str(cfg1_manifest))["passed"] is True
        assert parse_calls == 1

        # Same dir, now needing catalog -> upgrades entry in-place without evicting.
        assert mcp_server.run_checks(config_file=str(cfg1_catalog))["passed"] is True
        assert parse_calls == 2

        # Different dir exceeds cap=1 -> evicts t1 and caches t2.
        assert mcp_server.run_checks(config_file=str(cfg2_manifest))["passed"] is True
        assert parse_calls == 3
        assert len(mcp_server._ARTIFACT_CACHE) == 1

    def test_info_schema_signature_missing_dir_and_unreadable_file(
        self, tmp_path, monkeypatch
    ):
        """Missing info_schema directory returns None; unstat-able parquet files are skipped."""
        assert mcp_server._info_schema_signature(tmp_path) is None

        schema_dir = tmp_path / "info_schema" / "v1"
        schema_dir.mkdir(parents=True)
        (schema_dir / "dbt.project.parquet").write_bytes(b"")
        monkeypatch.setattr(mcp_server, "_file_signature", lambda _p: None)
        assert mcp_server._info_schema_signature(tmp_path) == ()

    def test_get_or_parse_artifacts_skips_caching_when_manifest_missing_stat(
        self, tmp_path, monkeypatch
    ):
        """If manifest cannot be statted before parse, the result is returned without caching."""
        import dbt_bouncer.artifact_parsers.parser as parser_mod

        fake_artifacts = SimpleNamespace()
        monkeypatch.setattr(
            parser_mod, "parse_dbt_artifacts", lambda **_kwargs: fake_artifacts
        )
        cfg = SimpleNamespace(
            package_name=None,
            catalog_checks=[],
            run_results_checks=[],
            info_schema_checks=[],
        )

        result = mcp_server._get_or_parse_artifacts(cfg, tmp_path)  # type: ignore[arg-type]

        assert result is fake_artifacts
        assert mcp_server._ARTIFACT_CACHE == {}


def test_build_server_registers_tools():
    """The MCP server registers all four tools."""
    pytest.importorskip("mcp")

    server = mcp_server.build_server()
    tools = asyncio.run(server.list_tools())

    assert sorted(t.name for t in tools) == [
        "explain_check",
        "list_checks",
        "read_project_config",
        "run_checks",
    ]


def test_build_server_with_fastmcp_layout(monkeypatch):
    """With the mcp<2 module layout, FastMCP is used and its Settings rebuilt."""
    pytest.importorskip("mcp")
    try:
        from mcp.server import MCPServer
    except ImportError:  # pragma: no cover - mcp<2 exercises this path natively
        pytest.skip("mcp<2 is installed, so the real FastMCP layout is used.")

    # `build_server` picks its server class and rebuilds FastMCP's `Settings`
    # only when `mcp.server.fastmcp` imports (the mcp<2 layout). mcp>=2 is
    # installed, so stub that layout in `sys.modules` to reach both branches.
    rebuilds: list[bool] = []
    fastmcp_module = ModuleType("mcp.server.fastmcp")
    fastmcp_module.FastMCP = MCPServer  # type: ignore[attr-defined]
    fastmcp_server_module = ModuleType("mcp.server.fastmcp.server")
    fastmcp_server_module.Settings = SimpleNamespace(  # type: ignore[attr-defined]
        model_rebuild=lambda: rebuilds.append(True)
    )
    monkeypatch.setitem(sys.modules, "mcp.server.fastmcp", fastmcp_module)
    monkeypatch.setitem(sys.modules, "mcp.server.fastmcp.server", fastmcp_server_module)

    server = mcp_server.build_server()

    assert isinstance(server, MCPServer)
    assert rebuilds == [True]


def test_mcp_command_runs_server(monkeypatch):
    """The mcp command builds the server and serves it."""
    calls: list[str] = []
    fake_server = SimpleNamespace(run=lambda: calls.append("run"))
    monkeypatch.setattr("dbt_bouncer.cli.mcp.server.build_server", lambda: fake_server)

    result = runner.invoke(app, ["mcp"])

    assert result.exit_code == 0
    assert calls == ["run"]


def test_mcp_command_without_dependency(monkeypatch):
    """The mcp command exits with CONFIG_ERROR when `mcp` is not installed."""

    # In production the ImportError is raised by importing `build_server` (the
    # `mcp` package is absent). We cannot easily un-import an installed package,
    # so we make the call raise instead: the command wraps both the import and
    # the call in one `try`, so either path hits the same CONFIG_ERROR handler.
    def raise_import_error():
        raise ImportError("No module named 'mcp'")

    monkeypatch.setattr("dbt_bouncer.cli.mcp.server.build_server", raise_import_error)

    result = runner.invoke(app, ["mcp"])

    assert result.exit_code == ExitCode.CONFIG_ERROR
