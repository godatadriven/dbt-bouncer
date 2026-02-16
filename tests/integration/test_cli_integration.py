from click.testing import CliRunner

from dbt_bouncer.main import cli


def test_list_checks_command():
    """Test list command shows available checks."""
    runner = CliRunner()
    result = runner.invoke(cli, ["list"])

    assert result.exit_code == 0
    assert "CheckModelDescriptionPopulated" in result.output


def test_cli_invalid_config_file():
    """Test CLI with non-existent config file."""
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            "non-existent-file.yml",
        ],
    )
    assert result.exit_code != 0


def test_cli_help():
    """Test CLI help output."""
    runner = CliRunner()
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "--config-file" in result.output


def test_init_help():
    """Test init command help output."""
    runner = CliRunner()
    result = runner.invoke(cli, ["init", "--help"])

    assert result.exit_code == 0
