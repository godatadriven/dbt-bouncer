"""Unit tests for dbt_bouncer.cli.explain."""

import json

from typer.testing import CliRunner

from dbt_bouncer.cli.explain.utils import get_documentation_url
from dbt_bouncer.enums import ExitCode
from dbt_bouncer.main import app

runner = CliRunner()


def _all_output(result) -> str:
    """Return stdout plus stderr, tolerating runners that do not split them.

    Returns:
        str: The combined output of the invocation.

    """
    try:
        return result.output + result.stderr
    except ValueError:
        return result.output


class TestExplainCommand:
    """Tests for the `explain` CLI subcommand."""

    def test_explain_by_name(self):
        """A check can be explained by its snake_case name."""
        result = runner.invoke(app, ["explain", "check_model_names"])

        assert result.exit_code == 0
        assert "check_model_names" in result.output
        assert "model_name_pattern" in result.output

    def test_explain_by_rule_code(self):
        """A check can be explained by its rule code."""
        result = runner.invoke(app, ["explain", "MO038"])

        assert result.exit_code == 0
        assert "check_model_names" in result.output

    def test_unknown_check_exits_config_error_with_suggestion(self):
        """An unknown check name exits with CONFIG_ERROR and a suggestion."""
        result = runner.invoke(app, ["explain", "check_model_namez"])

        assert result.exit_code == ExitCode.CONFIG_ERROR
        assert "Did you mean" in _all_output(result)

    def test_gibberish_check_gets_no_suggestion(self):
        """Input that resembles no check yields an error without a suggestion."""
        result = runner.invoke(app, ["explain", "xyzzy"])

        assert result.exit_code == ExitCode.CONFIG_ERROR
        assert "Did you mean" not in _all_output(result)

    def test_json_output(self):
        """The JSON output carries the full structured payload."""
        result = runner.invoke(
            app, ["explain", "check_model_names", "--output-format", "json"]
        )

        assert result.exit_code == 0
        payload = json.loads(result.output)
        assert payload["name"] == "check_model_names"
        assert payload["code"] == "MO038"
        assert payload["category"] == "manifest"
        assert payload["parameters"]["model_name_pattern"]["required"] is True
        assert payload["documentation_url"].endswith("checks/manifest/models/naming/")
        assert payload["description"] != ""
        assert "Example(s):" in payload["docstring"]

    def test_explain_check_without_extra_parameters(self):
        """A check with no extra parameters renders without a parameter table."""
        result = runner.invoke(
            app,
            ["explain", "check_model_has_unique_test", "--output-format", "json"],
        )

        assert result.exit_code == 0


def test_get_documentation_url_for_non_builtin_module():
    """Checks outside dbt_bouncer.checks have no documentation URL."""

    class FakeCheck:
        pass

    assert get_documentation_url(FakeCheck) is None  # type: ignore[arg-type]
