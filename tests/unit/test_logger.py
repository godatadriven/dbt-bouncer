import logging

import pytest
from click.testing import CliRunner

from dbt_bouncer.logger import configure_console_logging
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


def test_no_duplicate_handlers() -> None:
    """Calling configure_console_logging multiple times must not accumulate StreamHandlers."""
    root_logger = logging.getLogger("")
    handlers_before = [
        h for h in root_logger.handlers if type(h) is logging.StreamHandler
    ]

    configure_console_logging(verbosity=0)
    configure_console_logging(verbosity=0)
    configure_console_logging(verbosity=1)

    stream_handlers = [
        h for h in root_logger.handlers if type(h) is logging.StreamHandler
    ]
    assert len(stream_handlers) == 1, (
        f"Expected 1 StreamHandler after multiple configure_console_logging calls, "
        f"got {len(stream_handlers)}"
    )

    # Restore pre-test state
    for h in stream_handlers:
        root_logger.removeHandler(h)
    for h in handlers_before:
        root_logger.addHandler(h)
