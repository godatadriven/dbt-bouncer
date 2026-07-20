"""Tests for the Executor class."""

import logging

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


class _CountingCheckRunId:
    """A check_run_id stand-in that counts how often it's stringified.

    Used to prove the debug f-string body is never evaluated when DEBUG
    logging is disabled, rather than just checking no record was emitted.
    """

    def __init__(self):
        self.calls = 0

    def __str__(self):
        self.calls += 1
        return "counting_check:0"


def test_execute_batch_skips_debug_formatting_when_disabled():
    """With DEBUG disabled, the check_run_id f-string is never evaluated."""
    check_run_id = _CountingCheckRunId()
    checks = [
        {
            "check": _PassingCheck(),
            "check_run_id": check_run_id,
            "severity": CheckSeverity.ERROR,
        },
    ]
    root_logger = logging.getLogger()
    original_level = root_logger.level
    root_logger.setLevel(logging.INFO)
    try:
        Executor().run(checks)
    finally:
        root_logger.setLevel(original_level)
    assert check_run_id.calls == 0


def test_execute_batch_formats_debug_message_when_enabled():
    """With DEBUG enabled, the check_run_id is stringified for the debug log."""
    check_run_id = _CountingCheckRunId()
    checks = [
        {
            "check": _PassingCheck(),
            "check_run_id": check_run_id,
            "severity": CheckSeverity.ERROR,
        },
    ]
    root_logger = logging.getLogger()
    original_level = root_logger.level
    root_logger.setLevel(logging.DEBUG)
    try:
        Executor().run(checks)
    finally:
        root_logger.setLevel(original_level)
    assert check_run_id.calls >= 1


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
    assert "file_path" in result
    assert "outcome" in result
    assert "severity" in result
    assert "unique_id" in result


def test_executor_carries_file_path_and_unique_id():
    """file_path and unique_id from the input dict flow into the result."""
    checks = [
        {
            "check": _PassingCheck(),
            "check_run_id": "check_a:0:stg_orders",
            "file_path": "models/staging/stg_orders.sql",
            "severity": CheckSeverity.ERROR,
            "unique_id": "model.my_project.stg_orders",
        },
        # A context-only check has no resource -> file_path/unique_id absent.
        {
            "check": _PassingCheck(),
            "check_run_id": "check_b:0",
            "severity": CheckSeverity.ERROR,
        },
    ]
    executor = Executor()
    results = executor.run(checks)

    assert results[0]["file_path"] == "models/staging/stg_orders.sql"
    assert results[0]["unique_id"] == "model.my_project.stg_orders"
    # Missing keys default to None rather than raising.
    assert results[1]["file_path"] is None
    assert results[1]["unique_id"] is None
