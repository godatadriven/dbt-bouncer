import json
import logging

import pytest
from click.testing import CliRunner

from dbt_bouncer.logger import JsonFormatter, configure_console_logging
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


def test_json_formatter_outputs_valid_json(capsys) -> None:
    """JsonFormatter must output valid JSON."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("LOG_FORMAT", "json")

        configure_console_logging(verbosity=0)

        logger = logging.getLogger("test_json")
        logger.info("Test message")

        captured = capsys.readouterr()
        output = captured.err.strip()
        parsed = json.loads(output)

        assert "timestamp" in parsed
        assert parsed["level"] == "INFO"
        assert parsed["message"] == "Test message"
        assert "module" in parsed
        assert "function" in parsed
        assert "line" in parsed


def test_json_formatter_includes_exception_on_error(capsys) -> None:
    """JsonFormatter must include exception info when present.

    Raises:
        ValueError: For testing purposes.

    """
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("LOG_FORMAT", "json")

        configure_console_logging(verbosity=0)

        logger = logging.getLogger("test_json_exception")
        try:
            raise ValueError("Test error")
        except ValueError:
            logger.exception("An error occurred")

        captured = capsys.readouterr()
        output = captured.err.strip()
        parsed = json.loads(output)

        assert "exception" in parsed
        assert "ValueError" in parsed["exception"]
        assert "Test error" in parsed["exception"]


def test_json_format_env_var_enables_json() -> None:
    """LOG_FORMAT=json must enable JsonFormatter."""
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("LOG_FORMAT", "json")

        configure_console_logging(verbosity=0)

        # Verify JsonFormatter is used
        root_logger = logging.getLogger("")
        handler = root_logger.handlers[-1]
        assert isinstance(handler.formatter, JsonFormatter)


def test_default_format_is_custom_formatter() -> None:
    """Without LOG_FORMAT, CustomFormatter must be used."""
    with pytest.MonkeyPatch.context() as mp:
        mp.delenv("LOG_FORMAT", raising=False)

        configure_console_logging(verbosity=0)

        from dbt_bouncer.logger import CustomFormatter

        root_logger = logging.getLogger("")
        handler = root_logger.handlers[-1]
        assert isinstance(handler.formatter, CustomFormatter)
