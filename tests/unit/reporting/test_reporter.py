"""Tests for the Reporter class."""

from typing import TYPE_CHECKING, cast
from unittest.mock import patch

from dbt_bouncer.enums import CheckOutcome, CheckSeverity
from dbt_bouncer.reporting.reporter import Reporter

if TYPE_CHECKING:
    from dbt_bouncer.runner import CheckToRun


class _FakeCheck:
    """Minimal fake check for testing."""

    pass


def test_report_dry_run_returns_zero():
    """Dry run prints summary and returns exit code 0."""
    checks: list[CheckToRun] = [
        cast(
            "CheckToRun",
            {
                "check": _FakeCheck(),
                "check_run_id": "check_model_access:0:model.my_model",
                "severity": "error",
            },
        ),
    ]
    # Pass iterate cache as parameter -- no coupling to runner module
    iterate_cache = {_FakeCheck: frozenset({"model"})}

    reporter = Reporter(show_all_failures=False, create_pr_comment_file=False)
    exit_code, results = reporter.report_dry_run(checks, iterate_cache=iterate_cache)

    assert exit_code == 0
    assert results == []


def test_report_results_all_pass():
    """All checks pass -- returns exit code 0."""
    results = [
        {
            "check_run_id": "check_model_access:0:model.my_model",
            "failure_message": None,
            "outcome": CheckOutcome.SUCCESS,
            "severity": CheckSeverity.ERROR,
        },
    ]
    reporter = Reporter(show_all_failures=False, create_pr_comment_file=False)
    exit_code, returned = reporter.report_results(results)

    assert exit_code == 0
    assert returned == results


def test_report_results_with_errors():
    """Errors present -- returns exit code 1."""
    results = [
        {
            "check_run_id": "check_model_access:0:model.my_model",
            "failure_message": "Access should be private",
            "outcome": CheckOutcome.FAILED,
            "severity": CheckSeverity.ERROR,
        },
    ]
    reporter = Reporter(show_all_failures=False, create_pr_comment_file=False)
    exit_code, returned = reporter.report_results(results)

    assert exit_code == 1
    assert returned == results


def test_report_results_warnings_only():
    """Only warnings -- returns exit code 0."""
    results = [
        {
            "check_run_id": "check_model_access:0:model.my_model",
            "failure_message": "Some warning",
            "outcome": CheckOutcome.FAILED,
            "severity": CheckSeverity.WARN,
        },
    ]
    reporter = Reporter(show_all_failures=False, create_pr_comment_file=False)
    exit_code, returned = reporter.report_results(results)

    assert exit_code == 0
    assert returned == results


def test_report_results_saves_coverage_file(tmp_path):
    """Output file is written when configured."""
    output_file = tmp_path / "coverage.json"
    results = [
        {
            "check_run_id": "check_model_access:0:model.my_model",
            "failure_message": None,
            "outcome": CheckOutcome.SUCCESS,
            "severity": CheckSeverity.ERROR,
        },
    ]
    reporter = Reporter(
        show_all_failures=False,
        create_pr_comment_file=False,
        output_file=output_file,
        output_format="json",
    )
    reporter.report_results(results)

    assert output_file.exists()


def test_report_results_escapes_rich_markup_in_console(capsys, monkeypatch):
    """Failure messages containing `[...]` are not stripped by rich markup.

    Regression test for a display bug where regex character classes such as
    `[a-z0-9]` in a failure message were interpreted as rich style tags and
    silently removed from the console report (see issue #974).
    """
    # Force a wide console so the failure message renders on a single line and
    # the pattern isn't split across rich's column wrapping.
    monkeypatch.setenv("COLUMNS", "250")

    pattern = "^[a-z0-9]+(_[a-z0-9]+)*$"
    results = [
        {
            "check_run_id": "check_model_names:0:model.my_model",
            "failure_message": f"`my_model` does not match the supplied regex `{pattern}`.",
            "file_path": "models/staging/my_model.sql",
            "outcome": CheckOutcome.FAILED,
            "severity": CheckSeverity.ERROR,
        },
    ]
    reporter = Reporter(show_all_failures=True, create_pr_comment_file=False)
    reporter.report_results(results)

    out = capsys.readouterr().out
    assert "[a-z0-9]" in out


def test_report_results_does_not_substitute_emoji_shortcodes(capsys, monkeypatch):
    """Check run IDs containing `:<digits>:` are not turned into emoji.

    `check_run_id` is built as `<check_name>:<index>:<resource>`, so the 100th
    check of a run yields `...:100:...` — which rich reads as the shortcode for
    💯 and substitutes. `rich.markup.escape` does not prevent this (it only
    guards square-bracket tags), so the console is built with `emoji=False`.

    Beyond mangling the ID, the substituted character is unencodable in the
    cp1252 console Windows uses, raising `UnicodeEncodeError` and aborting the
    run outright.
    """
    monkeypatch.setenv("COLUMNS", "250")

    results = [
        {
            "check_run_id": "check_model_names:100:model.my_model",
            "failure_message": "`my_model` does not match the supplied regex.",
            "file_path": "models/staging/my_model.sql",
            "outcome": CheckOutcome.FAILED,
            "severity": CheckSeverity.ERROR,
        },
    ]
    reporter = Reporter(show_all_failures=True, create_pr_comment_file=False)
    reporter.report_results(results)

    out = capsys.readouterr().out
    assert ":100:" in out
    assert "\U0001f4af" not in out


def test_report_results_creates_pr_comment_file():
    """PR comment file is created when flag is set, with the file path column."""
    results = [
        {
            "check_run_id": "check_model_access:0:model.my_model",
            "failure_message": "Access should be private",
            "file_path": "models/staging/my_model.sql",
            "outcome": CheckOutcome.FAILED,
            "severity": CheckSeverity.ERROR,
            "unique_id": "model.my_project.my_model",
        },
    ]
    reporter = Reporter(show_all_failures=False, create_pr_comment_file=True)
    with patch("dbt_bouncer.reporting.reporter.create_github_comment_file") as mock_gh:
        reporter.report_results(results)
        mock_gh.assert_called_once()
        # Each row is [check_run_id, file_path, failure_message].
        rows = mock_gh.call_args.kwargs["failed_checks"]
        assert rows == [
            [
                "check_model_access:0:model.my_model",
                "models/staging/my_model.sql",
                "Access should be private",
            ]
        ]
