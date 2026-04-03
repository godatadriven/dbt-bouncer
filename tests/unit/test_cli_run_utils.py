"""Unit tests for dbt_bouncer.cli.run.utils."""

from pathlib import Path

import pytest

from dbt_bouncer.cli.run.utils import _detect_config_file_source
from dbt_bouncer.enums import ConfigFileName


class TestDetectConfigFileSource:
    """Tests for _detect_config_file_source."""

    def test_none_returns_default(self):
        """None config_file should return DEFAULT."""
        assert _detect_config_file_source(None) == "DEFAULT"

    def test_default_yml_returns_default(self):
        """The default YML path should return DEFAULT."""
        assert (
            _detect_config_file_source(Path(ConfigFileName.DBT_BOUNCER_YML))
            == "DEFAULT"
        )

    def test_default_toml_returns_default(self):
        """The default TOML path should return DEFAULT."""
        assert (
            _detect_config_file_source(Path(ConfigFileName.DBT_BOUNCER_TOML))
            == "DEFAULT"
        )

    def test_custom_path_returns_commandline(self):
        """A non-default path should return COMMANDLINE."""
        assert _detect_config_file_source(Path("custom-bouncer.yml")) == "COMMANDLINE"

    def test_nested_custom_path_returns_commandline(self):
        """A nested custom path should return COMMANDLINE."""
        assert (
            _detect_config_file_source(Path("config/my-bouncer.toml")) == "COMMANDLINE"
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
        assert _detect_config_file_source(config_file) == expected
