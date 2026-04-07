"""Unit tests for dbt_bouncer.cli.init.utils."""

from pathlib import Path

import yaml

from dbt_bouncer.cli.init.utils import build_initial_config, write_config_file


class TestBuildInitialConfig:
    """Tests for build_initial_config."""

    def test_no_checks_selected(self):
        """With all flags False, manifest_checks is empty and count is 0."""
        config, count = build_initial_config(
            artifacts_dir="target",
            check_descriptions=False,
            check_unique_tests=False,
            check_naming=False,
        )

        assert config == {"dbt_artifacts_dir": "target", "manifest_checks": []}
        assert count == 0

    def test_check_descriptions_only(self):
        """Enabling check_descriptions adds exactly one check."""
        config, count = build_initial_config(
            artifacts_dir="target",
            check_descriptions=True,
            check_unique_tests=False,
            check_naming=False,
        )

        assert count == 1
        names = [c["name"] for c in config["manifest_checks"]]
        assert "check_model_description_populated" in names
        assert "check_model_has_unique_test" not in names
        assert "check_model_names" not in names

    def test_check_unique_tests_only(self):
        """Enabling check_unique_tests adds exactly one check."""
        config, count = build_initial_config(
            artifacts_dir="target",
            check_descriptions=False,
            check_unique_tests=True,
            check_naming=False,
        )

        assert count == 1
        names = [c["name"] for c in config["manifest_checks"]]
        assert "check_model_has_unique_test" in names

    def test_check_naming_only(self):
        """Enabling check_naming adds exactly one check with include/model_name_pattern."""
        config, count = build_initial_config(
            artifacts_dir="target",
            check_descriptions=False,
            check_unique_tests=False,
            check_naming=True,
        )

        assert count == 1
        check = config["manifest_checks"][0]
        assert check["name"] == "check_model_names"
        assert check["include"] == "^models/staging"
        assert check["model_name_pattern"] == "^stg_"

    def test_all_checks_selected(self):
        """Enabling all flags results in 3 checks in the correct order."""
        config, count = build_initial_config(
            artifacts_dir="custom_dir",
            check_descriptions=True,
            check_unique_tests=True,
            check_naming=True,
        )

        assert count == 3
        names = [c["name"] for c in config["manifest_checks"]]
        assert names == [
            "check_model_description_populated",
            "check_model_has_unique_test",
            "check_model_names",
        ]

    def test_artifacts_dir_is_preserved(self):
        """The artifacts_dir argument is stored verbatim in the config."""
        config, _ = build_initial_config(
            artifacts_dir="my/custom/target",
            check_descriptions=False,
            check_unique_tests=False,
            check_naming=False,
        )

        assert config["dbt_artifacts_dir"] == "my/custom/target"

    def test_count_matches_manifest_checks_length(self):
        """The returned count always equals len(manifest_checks)."""
        for flags in [
            (True, False, True),
            (False, True, True),
            (True, True, False),
        ]:
            config, count = build_initial_config(
                artifacts_dir="target",
                check_descriptions=flags[0],
                check_unique_tests=flags[1],
                check_naming=flags[2],
            )
            assert count == len(config["manifest_checks"])


class TestWriteConfigFile:
    """Tests for write_config_file."""

    def test_creates_file(self, tmp_path, monkeypatch):
        """write_config_file creates dbt-bouncer.yml in the current directory."""
        monkeypatch.chdir(tmp_path)

        config = {"dbt_artifacts_dir": "target", "manifest_checks": []}
        path = write_config_file(config)

        assert path.exists()
        assert path.name == "dbt-bouncer.yml"

    def test_returns_path(self, tmp_path, monkeypatch):
        """write_config_file returns the Path of the written file."""
        monkeypatch.chdir(tmp_path)

        config = {"dbt_artifacts_dir": "target", "manifest_checks": []}
        result = write_config_file(config)

        assert isinstance(result, Path)

    def test_content_is_valid_yaml(self, tmp_path, monkeypatch):
        """The written file is valid YAML and round-trips correctly."""
        monkeypatch.chdir(tmp_path)

        config = {
            "dbt_artifacts_dir": "target",
            "manifest_checks": [{"name": "check_model_description_populated"}],
        }
        path = write_config_file(config)

        loaded = yaml.safe_load(path.read_text())
        assert loaded == config

    def test_key_order_is_preserved(self, tmp_path, monkeypatch):
        """sort_keys=False means dbt_artifacts_dir appears before manifest_checks."""
        monkeypatch.chdir(tmp_path)

        config = {
            "dbt_artifacts_dir": "target",
            "manifest_checks": [],
        }
        path = write_config_file(config)

        lines = path.read_text().splitlines()
        assert lines[0].startswith("dbt_artifacts_dir")

    def test_overwrites_existing_file(self, tmp_path, monkeypatch):
        """Calling write_config_file twice overwrites the first content."""
        monkeypatch.chdir(tmp_path)

        write_config_file({"dbt_artifacts_dir": "old", "manifest_checks": []})
        write_config_file({"dbt_artifacts_dir": "new", "manifest_checks": []})

        path = tmp_path / "dbt-bouncer.yml"
        content = path.read_text()
        assert "new" in content
        assert "old" not in content
