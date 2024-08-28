import pytest
from click.testing import CliRunner

from dbt_bouncer.main import cli


def test_logging_debug_cli(caplog) -> None:
    runner = CliRunner()
    runner.invoke(
        cli,
        ["--config-file", "dbt-bouncer-example.yml", "-v"],
    )
    assert "Running dbt-bouncer (0.0.0)..." in caplog.text
    assert len([r for r in caplog.messages if r.find("Loading config from") >= 0]) >= 2


def test_logging_debug_env_var(caplog) -> None:
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("LOG_LEVEL", "DEBUG")

        runner = CliRunner()
        runner.invoke(
            cli,
            ["--config-file", "dbt-bouncer-example.yml"],
        )
        assert "Running dbt-bouncer (0.0.0)..." in caplog.text
        assert (
            len([r for r in caplog.messages if r.find("Loading config from") >= 0]) >= 2
        )


def test_logging_info(caplog) -> None:
    runner = CliRunner()
    runner.invoke(
        cli,
        ["--config-file", "dbt-bouncer-example.yml"],
    )
    assert "Running dbt-bouncer (0.0.0)..." in caplog.text
