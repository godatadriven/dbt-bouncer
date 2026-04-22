"""Tests for the Executor class."""

from dbt_bouncer.enums import CheckOutcome, CheckSeverity
from dbt_bouncer.executor import Executor


class _PassingCheck:
    """A check that always passes."""

    description = None

    def execute(self):
        pass


class _FailingCheck:
    """A check that raises DbtBouncerFailedCheckError."""

    description = None

    def execute(self):
        from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError

        raise DbtBouncerFailedCheckError("Model access should be private")


class _CrashingCheck:
    """A check that raises an unexpected exception."""

    description = None

    def execute(self):
        raise ValueError("unexpected crash")


def test_executor_all_pass():
    """All checks pass -- outcomes are SUCCESS."""
    checks = [
        {
            "check": _PassingCheck(),
            "check_run_id": "check_a:0",
            "severity": CheckSeverity.ERROR,
        },
        {
            "check": _PassingCheck(),
            "check_run_id": "check_b:0",
            "severity": CheckSeverity.ERROR,
        },
    ]
    executor = Executor()
    results = executor.run(checks)
    assert len(results) == 2
    assert all(r["outcome"] == CheckOutcome.SUCCESS for r in results)


def test_executor_with_failure():
    """A failing check has FAILED outcome and error severity."""
    checks = [
        {
            "check": _FailingCheck(),
            "check_run_id": "check_a:0",
            "severity": CheckSeverity.ERROR,
        },
    ]
    executor = Executor()
    results = executor.run(checks)
    assert results[0]["outcome"] == CheckOutcome.FAILED
    assert results[0]["severity"] == CheckSeverity.ERROR
    assert "private" in results[0]["failure_message"]


def test_executor_unexpected_error_downgrades_to_warn():
    """Unexpected exceptions are downgraded to WARN severity."""
    checks = [
        {
            "check": _CrashingCheck(),
            "check_run_id": "check_a:0",
            "severity": CheckSeverity.ERROR,
        },
    ]
    executor = Executor()
    results = executor.run(checks)
    assert results[0]["outcome"] == CheckOutcome.FAILED
    assert results[0]["severity"] == CheckSeverity.WARN


def test_executor_empty_checks():
    """Empty check list returns empty results."""
    executor = Executor()
    results = executor.run([])
    assert results == []


def test_executor_returns_result_dicts():
    """Results contain expected keys."""
    checks = [
        {
            "check": _PassingCheck(),
            "check_run_id": "check_a:0",
            "severity": CheckSeverity.ERROR,
        },
    ]
    executor = Executor()
    results = executor.run(checks)
    result = results[0]
    assert "check_run_id" in result
    assert "outcome" in result
    assert "severity" in result
