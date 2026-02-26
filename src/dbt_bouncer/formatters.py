"""Output formatters for dbt-bouncer check results."""

import csv
import io
from typing import Any

import orjson


def _format_results(results: list[dict[str, Any]], output_format: str) -> bytes:
    """Serialise check results to the requested format.

    Args:
        results: List of check result dicts.
        output_format: One of "csv", "json", "junit", "sarif", or "tap".

    Returns:
        bytes: Serialised results.

    Raises:
        ValueError: If output_format is not recognised.

    """
    match output_format:
        case "csv":
            return _format_csv(results)
        case "json":
            return orjson.dumps(results)
        case "junit":
            return _format_junit(results)
        case "sarif":
            return _format_sarif(results)
        case "tap":
            return _format_tap(results)
        case _:
            msg = f"Unknown output format: {output_format}"
            raise ValueError(msg)


def _format_csv(results: list[dict[str, Any]]) -> bytes:
    """Serialise check results to CSV format.

    Args:
        results: List of check result dicts.

    Returns:
        bytes: CSV document.

    """
    buf = io.StringIO()
    fieldnames = ["check_run_id", "outcome", "severity", "failure_message"]
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(results)
    return buf.getvalue().encode()


def _format_junit(results: list[dict[str, Any]]) -> bytes:
    """Serialise check results to JUnit XML format.

    Each check result becomes a TestCase. Failed checks are marked with a
    <failure> element; warn-severity failures use type="warn".

    Args:
        results: List of check result dicts.

    Returns:
        bytes: JUnit XML document.

    """
    from junitparser import Failure, JUnitXml, TestCase, TestSuite

    test_cases = []
    for result in results:
        tc = TestCase(
            name=result["check_run_id"],
            classname="dbt-bouncer",
        )
        if result["outcome"] == "failed":
            tc.result = [
                Failure(
                    message=result.get("failure_message") or "",
                    type_=result.get("severity", "error"),
                )
            ]
        test_cases.append(tc)

    suite = TestSuite("dbt-bouncer")
    for tc in test_cases:
        suite.add_testcase(tc)

    xml = JUnitXml()
    xml.add_testsuite(suite)
    buf = io.BytesIO()
    xml.write(buf, pretty=True)
    return buf.getvalue()


def _format_sarif(results: list[dict[str, Any]]) -> bytes:
    """Serialise check results to SARIF 2.1.0 format.

    Args:
        results: List of check result dicts.

    Returns:
        bytes: SARIF JSON document.

    """
    sarif_results = []
    for r in results:
        level = "warning" if r.get("severity") == "warn" else "error"
        if r["outcome"] == "failed":
            sarif_results.append(
                {
                    "ruleId": r["check_run_id"],
                    "level": level,
                    "message": {"text": r.get("failure_message") or "Check failed"},
                }
            )
        else:
            sarif_results.append(
                {
                    "ruleId": r["check_run_id"],
                    "level": "none",
                    "message": {"text": "Check passed"},
                }
            )

    sarif = {
        "$schema": "https://raw.githubusercontent.com/oasis-tcs/sarif-spec/main/sarif-2.1/schema/sarif-schema-2.1.0.json",
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "dbt-bouncer",
                        "informationUri": "https://github.com/godatadriven/dbt-bouncer",
                    },
                },
                "results": sarif_results,
            },
        ],
    }
    return orjson.dumps(sarif, option=orjson.OPT_INDENT_2)


def _format_tap(results: list[dict[str, Any]]) -> bytes:
    """Serialise check results to TAP (Test Anything Protocol) format.

    Args:
        results: List of check result dicts.

    Returns:
        bytes: TAP document.

    """
    lines = ["TAP version 13", f"1..{len(results)}"]
    for i, r in enumerate(results, 1):
        status = "ok" if r["outcome"] != "failed" else "not ok"
        lines.append(f"{status} {i} - {r['check_run_id']}")
        if r["outcome"] == "failed" and r.get("failure_message"):
            for msg_line in r["failure_message"].splitlines():
                lines.append(f"  # {msg_line}")
    return "\n".join(lines).encode()
