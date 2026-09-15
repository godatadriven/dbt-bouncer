"""Integration tests for the CLI's help surface."""

import re

from dbt_bouncer.main import app


def strip_ansi(text: str) -> str:
    """Remove ANSI escape codes from text.

    Args:
        text: Text containing ANSI escape codes.

    Returns:
        Text with ANSI codes removed.

    """
    return re.sub(r"\x1b\[[0-9;]*m", "", text)


def test_cli_help(cli_runner):
    """Top-level `--help` documents `--config-file`."""
    result = cli_runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    # Strip ANSI codes since Rich may add them even in test environments
    assert "--config-file" in strip_ansi(result.output)


def test_init_help(cli_runner):
    """`init --help` exits cleanly."""
    result = cli_runner.invoke(app, ["init", "--help"])

    assert result.exit_code == 0
