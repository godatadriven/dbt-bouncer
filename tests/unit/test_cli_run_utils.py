"""Unit tests for dbt_bouncer.cli.run.utils."""

from pathlib import Path, PurePath
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import orjson
import pytest
from typer.testing import CliRunner

from dbt_bouncer.cli.run.utils import _build_context, detect_config_file_source
from dbt_bouncer.enums import ConfigFileName, ConfigFileSource, ExitCode
from dbt_bouncer.main import app
from dbt_bouncer.regression import build_baseline


class TestDetectConfigFileSource:
    """Tests for detect_config_file_source."""

    def test_none_returns_default(self):
        """None config_file should return DEFAULT."""
        assert detect_config_file_source(None) == ConfigFileSource.DEFAULT

    def test_default_yml_returns_default(self):
        """The default YML path should return DEFAULT."""
        assert (
            detect_config_file_source(Path(ConfigFileName.DBT_BOUNCER_YML))
            == ConfigFileSource.DEFAULT
        )

    def test_default_yaml_returns_default(self):
        """The default YAML path should return DEFAULT."""
        assert (
            detect_config_file_source(Path(ConfigFileName.DBT_BOUNCER_YAML))
            == ConfigFileSource.DEFAULT
        )

    def test_default_toml_returns_default(self):
        """The default TOML path should return DEFAULT."""
        assert (
            detect_config_file_source(Path(ConfigFileName.DBT_BOUNCER_TOML))
            == ConfigFileSource.DEFAULT
        )

    def test_custom_path_returns_commandline(self):
        """A non-default path should return COMMANDLINE."""
        assert (
            detect_config_file_source(Path("custom-bouncer.yml"))
            == ConfigFileSource.COMMANDLINE
        )

    def test_nested_custom_path_returns_commandline(self):
        """A nested custom path should return COMMANDLINE."""
        assert (
            detect_config_file_source(Path("config/my-bouncer.toml"))
            == ConfigFileSource.COMMANDLINE
        )

    @pytest.mark.parametrize(
        ("config_file", "expected"),
        [
            (Path("dbt-bouncer.yml"), ConfigFileSource.DEFAULT),
            (Path("dbt-bouncer.yaml"), ConfigFileSource.DEFAULT),
            (Path("dbt-bouncer.toml"), ConfigFileSource.DEFAULT),
            (Path("my-config.yml"), ConfigFileSource.COMMANDLINE),
            (None, ConfigFileSource.DEFAULT),
        ],
        ids=["default-yml", "default-yaml", "default-toml", "custom", "none"],
    )
    def test_parametrized(self, config_file: Path | None, expected: ConfigFileSource):
        """Parametrized coverage of all branches."""
        assert detect_config_file_source(config_file) == expected


class TestBuildContext:
    """Tests for _build_context."""

    @patch("dbt_bouncer.artifact_parsers.parser.parse_dbt_artifacts")
    def test_returns_bouncer_context(self, mock_parse: MagicMock):
        """_build_context should return a BouncerContext with all fields populated."""
        from dbt_bouncer.context import BouncerContext

        mock_parse.return_value = SimpleNamespace(
            catalog_nodes=[],
            catalog_sources=[],
            exposures=[],
            info_schema=None,
            macros=[],
            manifest_obj=MagicMock(),
            models=[],
            run_results=[],
            seeds=[],
            semantic_models=[],
            snapshots=[],
            sources=[],
            tests=[],
            unit_tests=[],
        )
        bouncer_config = MagicMock()

        ctx = _build_context(
            bouncer_config=bouncer_config,
            check_categories=["manifest_checks"],
            create_pr_comment_file=False,
            dbt_artifacts_dir=Path("target"),
            output_file=None,
            output_format="json",
            output_only_failures=False,
        )

        assert isinstance(ctx, BouncerContext)
        assert ctx.check_categories == ["manifest_checks"]
        assert ctx.create_pr_comment_file is False
        assert ctx.dry_run is False
        assert ctx.output_format == "json"
        assert ctx.output_only_failures is False
        assert ctx.show_all_failures is False
        mock_parse.assert_called_once_with(
            bouncer_config=bouncer_config, dbt_artifacts_dir=Path("target")
        )

    @patch("dbt_bouncer.artifact_parsers.parser.parse_dbt_artifacts")
    def test_dry_run_passed_through(self, mock_parse: MagicMock):
        """dry_run flag should be forwarded to the context."""
        mock_parse.return_value = SimpleNamespace(
            catalog_nodes=[],
            catalog_sources=[],
            exposures=[],
            info_schema=None,
            macros=[],
            manifest_obj=MagicMock(),
            models=[],
            run_results=[],
            seeds=[],
            semantic_models=[],
            snapshots=[],
            sources=[],
            tests=[],
            unit_tests=[],
        )

        ctx = _build_context(
            bouncer_config=MagicMock(),
            check_categories=[],
            create_pr_comment_file=False,
            dbt_artifacts_dir=Path("target"),
            dry_run=True,
            output_file=Path("out.json"),
            output_format="json",
            output_only_failures=True,
            show_all_failures=True,
        )

        assert ctx.dry_run is True
        assert ctx.output_file == Path("out.json")
        assert ctx.output_only_failures is True
        assert ctx.show_all_failures is True


class TestRunWithBaselineAndState:
    """Tests for `run --baseline` / `--state` (`_resolve_accepted_fingerprints`)."""

    @pytest.fixture
    def empty_baseline_file(self, tmp_path: Path) -> Path:
        """Write a baseline that accepts no failures.

        Returns:
            Path: The baseline file path.

        """
        baseline_file = tmp_path / "baseline.json"
        baseline_file.write_bytes(orjson.dumps(build_baseline([])))
        return baseline_file

    @staticmethod
    def _invoke_run(config_file: Path, *extra_args: str):
        return CliRunner().invoke(
            app,
            ["run", "--config-file", PurePath(config_file).as_posix(), *extra_args],
        )

    def test_baseline_without_state_keeps_new_failures(
        self, empty_baseline_file: Path, failing_config_file: Path
    ):
        """An empty baseline accepts nothing, so the failures still report."""
        result = self._invoke_run(
            failing_config_file, "--baseline", PurePath(empty_baseline_file).as_posix()
        )

        assert result.exit_code == ExitCode.CHECK_ERRORS

    def test_baseline_and_state_combined(
        self,
        dbt_112_target_dir: Path,
        empty_baseline_file: Path,
        failing_config_file: Path,
    ):
        """`--state` against the same artifacts suppresses every failure."""
        result = self._invoke_run(
            failing_config_file,
            "--baseline",
            PurePath(empty_baseline_file).as_posix(),
            "--state",
            dbt_112_target_dir.as_posix(),
        )

        assert result.exit_code == ExitCode.SUCCESS, result.output

    def test_state_not_a_directory_is_config_error(
        self, failing_config_file: Path, tmp_path: Path
    ):
        """`--state` pointing at a non-directory exits with CONFIG_ERROR."""
        result = self._invoke_run(
            failing_config_file, "--state", PurePath(tmp_path / "missing").as_posix()
        )

        assert result.exit_code == ExitCode.CONFIG_ERROR

    def test_state_without_artifacts_is_artifact_error(
        self, caplog, failing_config_file: Path, tmp_path: Path
    ):
        """`--state` at a directory without artifacts names `--state` as the cause."""
        empty_dir = tmp_path / "empty_target"
        empty_dir.mkdir()

        result = self._invoke_run(
            failing_config_file, "--state", PurePath(empty_dir).as_posix()
        )

        assert result.exit_code == ExitCode.ARTIFACT_ERROR
        assert "Failed to load the base artifacts from `--state" in caplog.text
