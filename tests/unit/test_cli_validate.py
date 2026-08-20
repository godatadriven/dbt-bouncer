"""Unit tests for dbt_bouncer.cli.validate."""

import yaml
from typer.testing import CliRunner

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app

runner = CliRunner()


def _write_yaml(path, content: dict) -> None:
    with path.open("w") as f:
        yaml.dump(content, f)


class TestValidateCommand:
    """Tests for the `validate` CLI subcommand."""

    def test_valid_config_exits_zero(self, tmp_path, monkeypatch):
        """A well-formed config file exits with code 0."""
        monkeypatch.chdir(tmp_path)
        _write_yaml(
            tmp_path / "dbt-bouncer.yml",
            {"manifest_checks": [{"name": "check_model_description_populated"}]},
        )

        result = runner.invoke(app, ["validate"])

        assert result.exit_code == 0

    def test_missing_config_file_exits_config_error(self, tmp_path, monkeypatch):
        """When the config file does not exist the CLI exits with CONFIG_ERROR."""
        monkeypatch.chdir(tmp_path)

        result = runner.invoke(app, ["validate"])

        assert result.exit_code == ExitCode.CONFIG_ERROR

    def test_invalid_config_exits_nonzero(self, tmp_path, monkeypatch):
        """A config with issues exits with a non-zero code."""
        monkeypatch.chdir(tmp_path)
        config_file = tmp_path / "dbt-bouncer.yml"
        # manifest_checks must be a list, not a string
        config_file.write_text("manifest_checks: not-a-list\n")

        result = runner.invoke(app, ["validate"])

        assert result.exit_code != 0

    def test_custom_config_file_path(self, tmp_path, monkeypatch):
        """--config-file accepts a custom path."""
        monkeypatch.chdir(tmp_path)
        config_file = tmp_path / "my_bouncer.yml"
        _write_yaml(
            config_file,
            {"manifest_checks": [{"name": "check_model_description_populated"}]},
        )

        result = runner.invoke(app, ["validate", "--config-file", str(config_file)])

        assert result.exit_code == 0

    def test_missing_name_field_exits_nonzero(self, tmp_path, monkeypatch):
        """A config where a check is missing the 'name' field exits non-zero."""
        monkeypatch.chdir(tmp_path)
        _write_yaml(
            tmp_path / "dbt-bouncer.yml",
            {"manifest_checks": [{"description": "no name here"}]},
        )

        result = runner.invoke(app, ["validate"])

        assert result.exit_code != 0

    def test_empty_config_exits_nonzero(self, tmp_path, monkeypatch):
        """An empty config file is reported as an issue."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "dbt-bouncer.yml").write_text("")

        result = runner.invoke(app, ["validate"])

        assert result.exit_code != 0

    def test_yaml_syntax_error_exits_nonzero(self, tmp_path, monkeypatch):
        """A YAML syntax error causes a non-zero exit."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "dbt-bouncer.yml").write_text(
            "manifest_checks:\n  - name: test\n    bad: [}\n"
        )

        result = runner.invoke(app, ["validate"])

        assert result.exit_code != 0

    def test_valid_config_outputs_success_message(self, tmp_path, monkeypatch):
        """A valid config prints a success message."""
        monkeypatch.chdir(tmp_path)
        _write_yaml(
            tmp_path / "dbt-bouncer.yml",
            {"manifest_checks": [{"name": "check_model_description_populated"}]},
        )

        result = runner.invoke(app, ["validate"])

        assert "Configuration file is valid!" in result.output

    def test_invalid_config_outputs_issue_count(self, tmp_path, monkeypatch):
        """An invalid config prints the number of issues found."""
        monkeypatch.chdir(tmp_path)
        config_file = tmp_path / "dbt-bouncer.yml"
        config_file.write_text("manifest_checks: not-a-list\n")

        result = runner.invoke(app, ["validate"])

        assert "Found" in result.output
        assert "issue(s) in config file:" in result.output

    def test_invalid_config_outputs_line_numbers(self, tmp_path, monkeypatch):
        """An invalid config prints line numbers for each issue."""
        monkeypatch.chdir(tmp_path)
        _write_yaml(
            tmp_path / "dbt-bouncer.yml",
            {"manifest_checks": [{"description": "no name here"}]},
        )

        result = runner.invoke(app, ["validate"])

        assert "Line" in result.output

    def test_unknown_top_level_key_reports_suggestion(self, tmp_path, monkeypatch):
        """A typo in a top-level key fails validation with a suggestion."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "dbt-bouncer.yml").write_text(
            "severtiy: warn\n"
            "manifest_checks:\n"
            "  - name: check_model_description_populated\n"
        )

        result = runner.invoke(app, ["validate"])

        assert result.exit_code == ExitCode.CHECK_ERRORS
        assert "severtiy" in result.output
        assert "Did you mean" in result.output

    def test_unknown_check_parameter_reports_line(self, tmp_path, monkeypatch):
        """A typo in a check parameter fails validation with its line number."""
        monkeypatch.chdir(tmp_path)
        (tmp_path / "dbt-bouncer.yml").write_text(
            "manifest_checks:\n"
            "  - name: check_model_names\n"
            "    model_name_patern: ^stg_\n"
        )

        result = runner.invoke(app, ["validate"])

        assert result.exit_code == ExitCode.CHECK_ERRORS
        assert "Line 3" in result.output
        assert "model_name_patern" in result.output

    def test_toml_config_is_fully_validated(self, tmp_path, monkeypatch):
        """A TOML config with an unknown key exits non-zero."""
        monkeypatch.chdir(tmp_path)
        config_file = tmp_path / "dbt-bouncer.toml"
        config_file.write_text(
            'severtiy = "warn"\n'
            "[[manifest_checks]]\n"
            'name = "check_model_description_populated"\n'
        )

        result = runner.invoke(app, ["validate", "--config-file", str(config_file)])

        assert result.exit_code == ExitCode.CHECK_ERRORS
        assert "severtiy" in result.output
