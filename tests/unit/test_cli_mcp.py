"""Unit tests for dbt_bouncer.cli.mcp."""

import asyncio
import os
import shutil
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
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


def _copy_target_artifacts(dst: Path, *, with_info_schema: bool = False) -> Path:
    """Copy committed dbt_project target artifacts into *dst*.

    Returns:
        Path: The populated artifact directory *dst*.

    """
    src = Path("dbt_project/target")
    dst.mkdir(parents=True, exist_ok=True)
    for name in ("manifest.json", "catalog.json", "run_results.json"):
        shutil.copy(src / name, dst / name)
    if with_info_schema:
        shutil.copytree(src / "info_schema", dst / "info_schema")
    return dst


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

        target = _copy_target_artifacts(tmp_path / "target", with_info_schema=True)
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

        # First call parses; identical and subset calls hit the cache.
        assert mcp_server.run_checks(config_file=str(cfg))["passed"] is True
        assert mcp_server.run_checks(config_file=str(cfg))["passed"] is True
        assert (
            mcp_server.run_checks(config_file=str(cfg), only="manifest_checks")[
                "passed"
            ]
            is True
        )
        assert parse_calls == 1

        # Bumping manifest.json mtime invalidates the cache.
        manifest_path = target / "manifest.json"
        st_info = manifest_path.stat()
        os.utime(
            manifest_path, ns=(st_info.st_atime_ns, st_info.st_mtime_ns + 1_000_000)
        )

        assert mcp_server.run_checks(config_file=str(cfg))["passed"] is True
        assert parse_calls == 2

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


_CACHE_OP_STRATEGY = st.tuples(
    st.integers(min_value=0, max_value=2),  # project index (3 dirs, cap=2)
    st.booleans(),  # need_catalog
    st.booleans(),  # need_run_results
    st.booleans(),  # need_info_schema
    st.sampled_from(
        [None, "manifest.json", "catalog.json", "run_results.json", "info_schema"]
    ),  # file to mutate before the call
)


class TestMcpArtifactCacheProperties:
    """Property-based state-machine tests for the MCP in-memory artifact cache."""

    @settings(max_examples=40)
    @given(ops=st.lists(_CACHE_OP_STRATEGY, min_size=1, max_size=12))
    def test_artifact_cache_freshness_reuse_and_bounded_size(
        self, tmp_path_factory, ops
    ):
        """Under arbitrary project/category/mutation sequences, the cache never returns stale data, never exceeds its cap, and reuses valid entries."""
        import dbt_bouncer.artifact_parsers.parser as parser_mod

        mcp_server.clear_artifact_cache()
        max_entries = 2

        root = tmp_path_factory.mktemp("mcp_cache_prop")
        projects: list[Path] = []
        # Track monotonically increasing synthetic version per (project, file)
        # so changing a file changes its size (guaranteed distinct stat signature).
        versions: list[dict[str, int]] = []
        for idx in range(3):
            pdir = root / f"proj_{idx}"
            (pdir / "info_schema" / "v1").mkdir(parents=True)
            v = {
                "manifest.json": 1,
                "catalog.json": 1,
                "run_results.json": 1,
                "info_schema": 1,
            }
            (pdir / "manifest.json").write_bytes(b"m")
            (pdir / "catalog.json").write_bytes(b"c")
            (pdir / "run_results.json").write_bytes(b"r")
            (pdir / "info_schema" / "v1" / "dbt.project.parquet").write_bytes(b"i")
            projects.append(pdir)
            versions.append(v)

        parse_calls = 0

        def fake_parse(bouncer_config, dbt_artifacts_dir):
            nonlocal parse_calls
            parse_calls += 1
            idx = projects.index(dbt_artifacts_dir)
            return SimpleNamespace(
                project_idx=idx,
                manifest_ver=versions[idx]["manifest.json"],
                catalog_ver=versions[idx]["catalog.json"]
                if bouncer_config.catalog_checks
                else None,
                run_results_ver=versions[idx]["run_results.json"]
                if bouncer_config.run_results_checks
                else None,
                info_schema_ver=versions[idx]["info_schema"]
                if bouncer_config.info_schema_checks
                else None,
            )

        # Reference model of what should be in the bounded cache.
        model_cache: dict[int, tuple[int, int | None, int | None, int | None]] = {}

        with pytest.MonkeyPatch.context() as mp:
            mp.setattr(mcp_server, "_MAX_ARTIFACT_CACHE_ENTRIES", max_entries)
            mp.setattr(parser_mod, "parse_dbt_artifacts", fake_parse)

            for proj_idx, need_cat, need_rr, need_is, mutate in ops:
                pdir = projects[proj_idx]
                if mutate is not None:
                    versions[proj_idx][mutate] += 1
                    payload = b"x" * versions[proj_idx][mutate]
                    if mutate == "info_schema":
                        (
                            pdir / "info_schema" / "v1" / "dbt.project.parquet"
                        ).write_bytes(payload)
                    else:
                        (pdir / mutate).write_bytes(payload)

                cfg = SimpleNamespace(
                    package_name=None,
                    catalog_checks=[object()] if need_cat else [],
                    run_results_checks=[object()] if need_rr else [],
                    info_schema_checks=[object()] if need_is else [],
                )

                expected_hit = False
                if proj_idx in model_cache:
                    m_ver, c_ver, r_ver, i_ver = model_cache[proj_idx]
                    expected_hit = (
                        m_ver == versions[proj_idx]["manifest.json"]
                        and (
                            not need_cat or c_ver == versions[proj_idx]["catalog.json"]
                        )
                        and (
                            not need_rr
                            or r_ver == versions[proj_idx]["run_results.json"]
                        )
                        and (not need_is or i_ver == versions[proj_idx]["info_schema"])
                    )

                calls_before = parse_calls
                result = mcp_server._get_or_parse_artifacts(cfg, pdir)  # type: ignore[arg-type]

                if expected_hit:
                    assert parse_calls == calls_before
                else:
                    assert parse_calls == calls_before + 1
                    if len(model_cache) >= max_entries and proj_idx not in model_cache:
                        del model_cache[next(iter(model_cache))]
                    model_cache[proj_idx] = (
                        versions[proj_idx]["manifest.json"],
                        versions[proj_idx]["catalog.json"] if need_cat else None,
                        versions[proj_idx]["run_results.json"] if need_rr else None,
                        versions[proj_idx]["info_schema"] if need_is else None,
                    )

                # Returned artifacts must always reflect the latest version of every requested file.
                assert result.manifest_ver == versions[proj_idx]["manifest.json"]
                if need_cat:
                    assert result.catalog_ver == versions[proj_idx]["catalog.json"]
                if need_rr:
                    assert (
                        result.run_results_ver == versions[proj_idx]["run_results.json"]
                    )
                if need_is:
                    assert result.info_schema_ver == versions[proj_idx]["info_schema"]

                # Cache size never exceeds its configured bound.
                assert len(mcp_server._ARTIFACT_CACHE) <= max_entries


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
