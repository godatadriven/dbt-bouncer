"""Unit tests for dbt_bouncer.cli.run.utils."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dbt_bouncer.cli.run.utils import _build_context, detect_config_file_source
from dbt_bouncer.enums import ConfigFileName


class TestDetectConfigFileSource:
    """Tests for detect_config_file_source."""

    def test_none_returns_default(self):
        """None config_file should return DEFAULT."""
        assert detect_config_file_source(None) == "DEFAULT"

    def test_default_yml_returns_default(self):
        """The default YML path should return DEFAULT."""
        assert (
            detect_config_file_source(Path(ConfigFileName.DBT_BOUNCER_YML)) == "DEFAULT"
        )

    def test_default_toml_returns_default(self):
        """The default TOML path should return DEFAULT."""
        assert (
            detect_config_file_source(Path(ConfigFileName.DBT_BOUNCER_TOML))
            == "DEFAULT"
        )

    def test_custom_path_returns_commandline(self):
        """A non-default path should return COMMANDLINE."""
        assert detect_config_file_source(Path("custom-bouncer.yml")) == "COMMANDLINE"

    def test_nested_custom_path_returns_commandline(self):
        """A nested custom path should return COMMANDLINE."""
        assert (
            detect_config_file_source(Path("config/my-bouncer.toml")) == "COMMANDLINE"
        )

    @pytest.mark.parametrize(
        ("config_file", "expected"),
        [
            (Path("dbt-bouncer.yml"), "DEFAULT"),
            (Path("dbt-bouncer.toml"), "DEFAULT"),
            (Path("my-config.yml"), "COMMANDLINE"),
            (None, "DEFAULT"),
        ],
        ids=["default-yml", "default-toml", "custom", "none"],
    )
    def test_parametrized(self, config_file: Path | None, expected: str):
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
