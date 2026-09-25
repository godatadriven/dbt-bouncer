"""Integration tests for `--output-file` in every `--output-format`.

The formatter unit tests feed `_format_results` hand-built result dicts. These
tests run real checks through the CLI and parse the file each format writes
with a reader for that format, so a change to the shape of the executor's
result dicts that a formatter no longer understands fails here.
"""

import csv
import json
import xml.etree.ElementTree as ET  # ruff: ignore[suspicious-xml-etree-import]
from pathlib import PurePath

import pytest

from dbt_bouncer.enums import ExitCode, OutputFormat
from dbt_bouncer.main import app

_FAILING_CHECK = {
    "name": "check_model_names",
    "include": r"models/marts/finance/orders\.sql",
    "model_name_pattern": "^stg_",
}
_PASSING_CHECK = {
    "name": "check_model_names",
    "include": r"models/staging/crm/stg_orders\.sql",
    "model_name_pattern": "^stg_",
}

# `check_run_id` is `<check name>:<index in config>:<resource>`, and the passing
# check is configured first.
_FAILING_ID = "check_model_names:1:orders"
_FAILING_MESSAGE = "`orders` does not match the supplied regex `^stg_`."
_PASSING_ID = "check_model_names:0:stg_orders"


def _parse_csv(text: str) -> dict[str, str | None]:
    return {
        row["check_run_id"]: row["failure_message"]
        if row["outcome"] == "failed"
        else None
        for row in csv.DictReader(text.splitlines())
    }


def _parse_json(text: str) -> dict[str, str | None]:
    return {
        r["check_run_id"]: r["failure_message"] if r["outcome"] == "failed" else None
        for r in json.loads(text)
    }


def _junit_suite(text: str) -> ET.Element:
    # The XML is the file the test itself just wrote, not untrusted input.
    root = ET.fromstring(text)  # ruff: ignore[suspicious-xml-element-tree-usage]
    suite = root.find("testsuite")
    assert suite is not None
    return suite


def _parse_junit(text: str) -> dict[str, str | None]:
    suite = _junit_suite(text)
    assert suite.get("name") == "dbt-bouncer"
    parsed = {}
    for case in suite.iter("testcase"):
        failure = case.find("failure")
        parsed[case.get("name", "")] = (
            None if failure is None else failure.get("message")
        )
    return parsed


def _parse_sarif(text: str) -> dict[str, str | None]:
    sarif = json.loads(text)
    assert sarif["version"] == "2.1.0"
    (run,) = sarif["runs"]
    assert run["tool"]["driver"]["name"] == "dbt-bouncer"
    return {
        r["ruleId"]: None if r["level"] == "none" else r["message"]["text"]
        for r in run["results"]
    }


def _parse_tap(text: str) -> dict[str, str | None]:
    lines = text.splitlines()
    assert lines[0] == "TAP version 13"
    parsed: dict[str, str | None] = {}
    current = ""
    for line in lines[2:]:
        if line.startswith("  # "):
            parsed[current] = line.removeprefix("  # ")
            continue
        status, _, rest = line.partition(" - ")
        current = rest
        parsed[current] = None
        assert status.startswith(("ok ", "not ok "))
    assert lines[1] == f"1..{len(parsed)}"
    return parsed


# Each parser returns `{check_run_id: failure message, or None if it passed}`.
_PARSERS = {
    OutputFormat.CSV: _parse_csv,
    OutputFormat.JSON: _parse_json,
    OutputFormat.JUNIT: _parse_junit,
    OutputFormat.SARIF: _parse_sarif,
    OutputFormat.TAP: _parse_tap,
}

_FORMATS = pytest.mark.parametrize(
    "output_format", list(_PARSERS), ids=[f.value for f in _PARSERS]
)


def _run(cli_runner, config_file, output_file, output_format, *extra_args):
    """Invoke `dbt-bouncer run` writing `output_file` in `output_format`.

    Returns:
        Result: The `CliRunner` result.

    """
    return cli_runner.invoke(
        app,
        [
            "run",
            "--config-file",
            PurePath(config_file).as_posix(),
            "--output-file",
            str(output_file),
            "--output-format",
            output_format.value,
            *extra_args,
        ],
    )


def test_every_output_format_is_covered():
    """Adding an `OutputFormat` without a parser here would silently skip it."""
    assert set(_PARSERS) == set(OutputFormat)


@_FORMATS
def test_output_file_records_every_result(
    cli_runner, output_format, tmp_path, write_checks_config
):
    """The output file holds the passing and the failing result, in the chosen format."""
    config_file = write_checks_config([_PASSING_CHECK, _FAILING_CHECK])
    output_file = tmp_path / f"results.{output_format.value}"

    result = _run(cli_runner, config_file, output_file, output_format)

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    parsed = _PARSERS[output_format](output_file.read_text(encoding="utf-8"))
    assert parsed == {_FAILING_ID: _FAILING_MESSAGE, _PASSING_ID: None}


@_FORMATS
def test_output_only_failures_drops_passing_results(
    cli_runner, output_format, tmp_path, write_checks_config
):
    """`--output-only-failures` keeps only the failing result in the output file."""
    config_file = write_checks_config([_PASSING_CHECK, _FAILING_CHECK])
    output_file = tmp_path / f"results.{output_format.value}"

    result = _run(
        cli_runner, config_file, output_file, output_format, "--output-only-failures"
    )

    assert result.exit_code == ExitCode.CHECK_ERRORS, result.output
    parsed = _PARSERS[output_format](output_file.read_text(encoding="utf-8"))
    assert parsed == {_FAILING_ID: _FAILING_MESSAGE}


@_FORMATS
def test_output_only_failures_on_a_passing_run_writes_an_empty_document(
    cli_runner, output_format, tmp_path, write_checks_config
):
    """With nothing failing, the file is still written and still parses, just empty."""
    config_file = write_checks_config([_PASSING_CHECK])
    output_file = tmp_path / f"results.{output_format.value}"

    result = _run(
        cli_runner, config_file, output_file, output_format, "--output-only-failures"
    )

    assert result.exit_code == ExitCode.SUCCESS, result.output
    assert _PARSERS[output_format](output_file.read_text(encoding="utf-8")) == {}


def test_sarif_locates_failures_for_code_scanning(
    cli_runner, tmp_path, write_checks_config
):
    """SARIF results carry the file and resource GitHub code scanning anchors on."""
    config_file = write_checks_config([_FAILING_CHECK])
    output_file = tmp_path / "results.sarif"

    _run(cli_runner, config_file, output_file, OutputFormat.SARIF)

    (entry,) = json.loads(output_file.read_text(encoding="utf-8"))["runs"][0]["results"]
    assert entry["level"] == "error"
    assert entry["locations"] == [
        {
            "physicalLocation": {
                "artifactLocation": {"uri": "models/marts/finance/orders.sql"},
                "region": {"startLine": 1},
            }
        }
    ]
    assert entry["logicalLocations"] == [
        {"fullyQualifiedName": "model.dbt_bouncer_test_project.orders"}
    ]


def test_warn_severity_is_carried_into_junit_and_sarif(
    cli_runner, tmp_path, write_checks_config
):
    """A `warn` failure is typed `warn` in JUnit and `warning` in SARIF, not `error`."""
    config_file = write_checks_config([{**_FAILING_CHECK, "severity": "warn"}])
    junit_file = tmp_path / "results.xml"
    sarif_file = tmp_path / "results.sarif"

    junit = _run(cli_runner, config_file, junit_file, OutputFormat.JUNIT)
    sarif = _run(cli_runner, config_file, sarif_file, OutputFormat.SARIF)

    assert junit.exit_code == sarif.exit_code == ExitCode.SUCCESS
    failure = _junit_suite(junit_file.read_text(encoding="utf-8")).find(
        "testcase/failure"
    )
    assert failure is not None
    assert failure.get("type") == "warn"
    (entry,) = json.loads(sarif_file.read_text(encoding="utf-8"))["runs"][0]["results"]
    assert entry["level"] == "warning"
