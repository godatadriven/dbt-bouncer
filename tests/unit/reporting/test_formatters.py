"""Tests for the output formatters, focusing on file-location plumbing."""

import orjson

from dbt_bouncer.enums import CheckOutcome, CheckSeverity
from dbt_bouncer.reporting.formatters import (
    _format_csv,
    _format_junit,
    _format_sarif,
)


def _failed_result(**overrides):
    result = {
        "check_run_id": "check_model_description_populated:0:stg_orders",
        "failure_message": "Model has no description",
        "file_path": "models/staging/stg_orders.sql",
        "outcome": CheckOutcome.FAILED,
        "severity": CheckSeverity.ERROR,
        "unique_id": "model.my_project.stg_orders",
    }
    result.update(overrides)
    return result


def test_sarif_includes_location_and_logical_location():
    """A result with a file_path gets a physical + logical location."""
    sarif = orjson.loads(_format_sarif([_failed_result()]))
    entry = sarif["runs"][0]["results"][0]

    location = entry["locations"][0]["physicalLocation"]
    assert location["artifactLocation"]["uri"] == "models/staging/stg_orders.sql"
    assert location["region"]["startLine"] == 1
    assert entry["logicalLocations"][0]["fullyQualifiedName"] == (
        "model.my_project.stg_orders"
    )
    # ruleId is unchanged (per-run id).
    assert entry["ruleId"] == "check_model_description_populated:0:stg_orders"


def test_sarif_includes_location_for_passing_result():
    """A passing result with a file_path still attaches locations (level="none")."""
    result = _failed_result(outcome=CheckOutcome.SUCCESS)
    sarif = orjson.loads(_format_sarif([result]))
    entry = sarif["runs"][0]["results"][0]

    assert entry["level"] == "none"
    location = entry["locations"][0]["physicalLocation"]
    assert location["artifactLocation"]["uri"] == "models/staging/stg_orders.sql"
    assert entry["logicalLocations"][0]["fullyQualifiedName"] == (
        "model.my_project.stg_orders"
    )


def test_sarif_omits_location_when_no_file_path():
    """A context-only result (no file_path) omits locations entirely."""
    result = _failed_result(file_path=None, unique_id=None)
    sarif = orjson.loads(_format_sarif([result]))
    entry = sarif["runs"][0]["results"][0]

    assert "locations" not in entry
    assert "logicalLocations" not in entry


def test_junit_sets_file_attribute():
    """A failed result renders <testcase ... file="...">."""
    xml = _format_junit([_failed_result()]).decode()
    assert 'file="models/staging/stg_orders.sql"' in xml


def test_junit_omits_file_attribute_when_no_file_path():
    """A result without a file_path has no file attribute."""
    xml = _format_junit([_failed_result(file_path=None)]).decode()
    assert "file=" not in xml


def test_csv_includes_file_path_and_unique_id_columns():
    """CSV output exposes the new file_path and unique_id columns."""
    csv_text = _format_csv([_failed_result()]).decode()
    header = csv_text.splitlines()[0]
    assert "file_path" in header
    assert "unique_id" in header
    assert "models/staging/stg_orders.sql" in csv_text
    assert "model.my_project.stg_orders" in csv_text
