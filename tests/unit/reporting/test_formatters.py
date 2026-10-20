"""Tests for the output formatters, focusing on file-location plumbing."""

import csv
import io

import orjson
from hypothesis import given, settings
from hypothesis import strategies as st
from junitparser import JUnitXml

from dbt_bouncer.enums import CheckOutcome, CheckSeverity
from dbt_bouncer.reporting.formatters import (
    _format_csv,
    _format_gitlab,
    _format_junit,
    _format_results,
    _format_sarif,
    _format_tap,
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


def test_gitlab_reports_failure_at_file_line_1():
    """A failed result becomes a major Code Quality finding on line 1."""
    (finding,) = orjson.loads(_format_gitlab([_failed_result()]))

    assert finding["check_name"] == "check_model_description_populated"
    assert finding["description"] == (
        "check_model_description_populated: Model has no description"
    )
    assert finding["severity"] == "major"
    assert finding["location"] == {
        "path": "models/staging/stg_orders.sql",
        "lines": {"begin": 1},
    }


def test_gitlab_omits_passing_results():
    """Code Quality has no passing state, so passes are left out."""
    assert (
        orjson.loads(_format_gitlab([_failed_result(outcome=CheckOutcome.SUCCESS)]))
        == []
    )


def test_gitlab_maps_warn_to_minor_and_reports_internal_error():
    """Warn failures are `minor`; a crashed check is still reported."""
    warn, crashed = orjson.loads(
        _format_gitlab(
            [
                _failed_result(severity=CheckSeverity.WARN),
                _failed_result(outcome=CheckOutcome.INTERNAL_ERROR),
            ]
        )
    )
    assert warn["severity"] == "minor"
    assert crashed["severity"] == "major"


def test_gitlab_falls_back_to_dbt_project_yml_without_file_path():
    """Code Quality requires a path, so context-only results use the project file."""
    (finding,) = orjson.loads(
        _format_gitlab([_failed_result(file_path=None, unique_id=None)])
    )
    assert finding["location"]["path"] == "dbt_project.yml"


def test_gitlab_fingerprint_ignores_check_index():
    """Reordering the config must not make every finding look new."""
    first, reordered, other = orjson.loads(
        _format_gitlab(
            [
                _failed_result(),
                _failed_result(
                    check_run_id="check_model_description_populated:7:stg_orders"
                ),
                _failed_result(unique_id="model.my_project.stg_customers"),
            ]
        )
    )
    assert first["fingerprint"] == reordered["fingerprint"]
    assert first["fingerprint"] != other["fingerprint"]


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


def test_junit_marks_internal_error_with_error_element():
    """A crashed check is a JUnit <error>, distinct from a <failure>."""
    xml = _format_junit(
        [_failed_result(outcome=CheckOutcome.INTERNAL_ERROR, failure_message="boom")]
    ).decode()
    assert "<error" in xml
    assert "<failure" not in xml
    assert 'message="boom"' in xml


def test_sarif_reports_internal_error_at_its_severity():
    """A crashed check is reported, never rendered as `Check passed`."""
    sarif = orjson.loads(
        _format_sarif(
            [
                _failed_result(
                    outcome=CheckOutcome.INTERNAL_ERROR, failure_message="boom"
                )
            ]
        )
    )
    entry = sarif["runs"][0]["results"][0]
    assert entry["level"] == "error"
    assert entry["message"]["text"] == "boom"


def test_tap_marks_internal_error_not_ok():
    """TAP reports a crashed check as `not ok`, with its message."""
    tap = _format_tap(
        [_failed_result(outcome=CheckOutcome.INTERNAL_ERROR, failure_message="boom")]
    ).decode()
    assert "not ok 1 - check_model_description_populated:0:stg_orders" in tap
    assert "  # boom" in tap


_VALID_XML_TEXT = st.text(
    alphabet=st.characters(
        blacklist_categories=("Cs",),
        blacklist_characters="".join(chr(c) for c in range(32) if c not in (9, 10, 13)),
    ),
    max_size=100,
)

_ADVERSARIAL_TEXT = st.one_of(
    st.none(),
    _VALID_XML_TEXT,
    st.sampled_from(
        [
            "<script>alert(1)</script>",
            "</failure></testcase>",
            'foo,bar\r\nbaz"quote"',
            "ok 1 - fake tap",
            "not ok 2 - fake tap",
            "&amp; &lt; &gt; &quot; &#39;",
            "emoji 🎉🚀💥 \t\n",
            "\n\n\n",
        ]
    ),
)

_CHECK_RESULT_STRATEGY = st.fixed_dictionaries(
    {
        "check_run_id": st.from_regex(r"check_[a-z]+:[0-9]+", fullmatch=True),
        "failure_message": _ADVERSARIAL_TEXT,
        "file_path": st.one_of(
            st.none(),
            st.from_regex(r"models/[a-z_]+\.sql", fullmatch=True),
        ),
        "outcome": st.sampled_from(list(CheckOutcome)),
        "severity": st.sampled_from(list(CheckSeverity)),
        "unique_id": st.one_of(
            st.none(),
            st.from_regex(r"model\.p\.[a-z_]+", fullmatch=True),
        ),
    }
)


class TestFormatterProperties:
    """Property-based tests for output formatters."""

    @settings(max_examples=50)
    @given(results=st.lists(_CHECK_RESULT_STRATEGY, max_size=15))
    def test_all_formatters_cardinality_and_parsing(self, results):
        """All formatters preserve result cardinality, field fidelity, and valid syntax under adversarial inputs."""
        # 1. JSON
        json_bytes = _format_results(results, "json")
        parsed_json = orjson.loads(json_bytes)
        assert len(parsed_json) == len(results)
        for item, orig in zip(parsed_json, results, strict=True):
            assert item["check_run_id"] == orig["check_run_id"]
            assert item["outcome"] == orig["outcome"]
            assert item["severity"] == orig["severity"]
            assert item["failure_message"] == orig["failure_message"]
            assert item["file_path"] == orig["file_path"]
            assert item["unique_id"] == orig["unique_id"]

        # 2. CSV
        csv_bytes = _format_results(results, "csv")
        csv_rows = list(csv.DictReader(io.StringIO(csv_bytes.decode())))
        assert len(csv_rows) == len(results)
        for row, orig in zip(csv_rows, results, strict=True):
            assert row["check_run_id"] == orig["check_run_id"]
            assert row["outcome"] == str(orig["outcome"])
            assert row["severity"] == str(orig["severity"])
            assert row["failure_message"] == (orig["failure_message"] or "")
            assert row["file_path"] == (orig["file_path"] or "")
            assert row["unique_id"] == (orig["unique_id"] or "")

        # 3. SARIF
        sarif = orjson.loads(_format_results(results, "sarif"))
        sarif_results = sarif["runs"][0]["results"]
        assert len(sarif_results) == len(results)
        for entry, orig in zip(sarif_results, results, strict=True):
            assert entry["ruleId"] == orig["check_run_id"]
            if orig["outcome"] == CheckOutcome.SUCCESS:
                assert entry["level"] == "none"
                assert entry["message"]["text"] == "Check passed"
            else:
                expected_level = (
                    "warning" if orig["severity"] == CheckSeverity.WARN else "error"
                )
                assert entry["level"] == expected_level
                assert entry["message"]["text"] == (
                    orig["failure_message"] or "Check failed"
                )
            assert ("locations" in entry) == bool(orig["file_path"])
            assert ("logicalLocations" in entry) == bool(orig["unique_id"])

        # 3b. GitLab Code Quality: only results that did not pass.
        gitlab = orjson.loads(_format_results(results, "gitlab"))
        non_passing = [r for r in results if r["outcome"] != CheckOutcome.SUCCESS]
        assert len(gitlab) == len(non_passing)
        for finding, orig in zip(gitlab, non_passing, strict=True):
            assert finding["check_name"] == orig["check_run_id"].split(":", 1)[0]
            assert finding["severity"] in ("minor", "major")
            assert len(finding["fingerprint"]) == 32
            assert finding["location"]["path"] == (
                orig["file_path"] or "dbt_project.yml"
            )
            assert finding["location"]["lines"] == {"begin": 1}

        # 4. TAP
        tap_text = _format_results(results, "tap").decode()
        lines = tap_text.splitlines()
        assert lines[0] == "TAP version 13"
        assert lines[1] == f"1..{len(results)}"
        test_lines = []
        for line in lines[2:]:
            if line.startswith(("ok ", "not ok ")):
                test_lines.append(line)
            else:
                assert line.startswith("  # ")
        assert len(test_lines) == len(results)
        for idx, (tline, orig) in enumerate(
            zip(test_lines, results, strict=True), start=1
        ):
            expected_prefix = (
                f"ok {idx} - "
                if orig["outcome"] == CheckOutcome.SUCCESS
                else f"not ok {idx} - "
            )
            assert tline == f"{expected_prefix}{orig['check_run_id']}"

        # 5. JUnit XML
        junit_bytes = _format_results(results, "junit")
        xml = JUnitXml.fromstring(junit_bytes)
        testcases = [tc for suite in xml for tc in suite]
        assert len(testcases) == len(results)
        for tc, orig in zip(testcases, results, strict=True):
            assert tc.name == orig["check_run_id"]
            if orig["outcome"] == CheckOutcome.SUCCESS:
                assert tc.result == []
            else:
                assert len(tc.result) == 1
