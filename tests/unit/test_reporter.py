"""Tests for the Reporter class."""

from unittest.mock import patch

from dbt_bouncer.enums import CheckOutcome, CheckSeverity
from dbt_bouncer.reporter import Reporter


class _FakeCheck:
    """Minimal fake check for testing."""

    pass


def test_report_dry_run_returns_zero():
    """Dry run prints summary and returns exit code 0."""
    checks = [
        {
            "check": _FakeCheck(),
            "check_run_id": "check_model_access:0:model.my_model",
            "severity": "error",
        },
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


def test_report_results_creates_pr_comment_file():
    """PR comment file is created when flag is set."""
    results = [
        {
            "check_run_id": "check_model_access:0:model.my_model",
            "failure_message": "Access should be private",
            "outcome": CheckOutcome.FAILED,
            "severity": CheckSeverity.ERROR,
        },
    ]
    reporter = Reporter(show_all_failures=False, create_pr_comment_file=True)
    with patch("dbt_bouncer.reporter.create_github_comment_file") as mock_gh:
        reporter.report_results(results)
        mock_gh.assert_called_once()
