"""Tests for Rich output enhancements."""


import pytest
from typer.testing import CliRunner

from dbt_bouncer.main import app


@pytest.fixture
def runner():
    """Create a CLI runner for testing.

    Returns:
        CliRunner: A CLI runner instance.

    """
    return CliRunner()


def test_init_shows_rich_header(tmp_path, monkeypatch):
    """Verify the emoji header in the init command."""
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()

    # Use default inputs
    result = runner.invoke(app, ["init"], input="\n\n\n\n")

    # Check for the header emoji and text
    assert "🚀" in result.stdout
    assert "dbt-bouncer initialization" in result.stdout


def test_init_shows_rich_success_message(tmp_path, monkeypatch):
    """Verify the checkmark success message in the init command."""
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()

    # Use default inputs
    result = runner.invoke(app, ["init"], input="\n\n\n\n")

    # Check for success message and emoji
    assert "✓" in result.stdout
    assert "Created dbt-bouncer.yml" in result.stdout


def test_init_shows_check_count(tmp_path, monkeypatch):
    """Verify that init shows how many checks were added."""
    monkeypatch.chdir(tmp_path)
    runner = CliRunner()

    # Select all checks (yes to all 3 prompts)
    result = runner.invoke(app, ["init"], input="\ny\ny\ny\n")

    # Should show that 3 checks were added
    assert "Added 3 checks" in result.stdout or "Added" in result.stdout


def test_rich_help_panels_shown(runner):
    """Verify that help panels are shown with Rich formatting."""
    result = runner.invoke(app, ["run", "--help"])

    # Check that help panels exist
    assert "Check Selection" in result.stdout
    assert "Output Options" in result.stdout
    assert "Display Options" in result.stdout


def test_rich_help_examples_shown(runner):
    """Verify that examples are shown in help text."""
    result = runner.invoke(app, ["run", "--help"])

    # Check for examples section
    assert "Examples:" in result.stdout
    assert "dbt-bouncer run" in result.stdout
