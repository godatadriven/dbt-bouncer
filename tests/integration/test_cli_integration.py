import re

from typer.testing import CliRunner

from dbt_bouncer.main import app


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text.

    Args:
        text: Text containing ANSI escape codes.

    Returns:
        Text with ANSI codes removed.

    """
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def test_list_checks_command():
    """Test list command shows available checks."""
    runner = CliRunner()
    result = runner.invoke(app, ["list"])

    assert result.exit_code == 0
    assert "CheckModelDescriptionPopulated" in result.output


def test_cli_invalid_config_file():
    """Test CLI with non-existent config file."""
    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "--config-file",
            "non-existent-file.yml",
        ],
    )
    assert result.exit_code != 0


def test_cli_help():
    """Test CLI help output."""
    runner = CliRunner()
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    # Strip ANSI codes since Rich may add them even in test environments
    clean_output = strip_ansi(result.output)
    assert "--config-file" in clean_output


def test_init_help():
    """Test init command help output."""
    runner = CliRunner()
    result = runner.invoke(app, ["init", "--help"])

    assert result.exit_code == 0
