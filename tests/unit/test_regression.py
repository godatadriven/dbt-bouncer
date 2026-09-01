"""Tests for the regression (baseline/state) filter."""

import orjson
import pytest

from dbt_bouncer.enums import CheckOutcome
from dbt_bouncer.exceptions import DbtBouncerConfigError
from dbt_bouncer.regression import (
    apply_regression_filter,
    build_baseline,
    failure_fingerprints,
    fingerprint,
    load_baseline,
)


def _failure(check_run_id, unique_id=None, file_path=None, message="failed"):
    return {
        "check_run_id": check_run_id,
        "failure_message": message,
        "file_path": file_path,
        "outcome": CheckOutcome.FAILED,
        "severity": "error",
        "unique_id": unique_id,
    }


def _success(check_run_id, unique_id=None):
    return {
        "check_run_id": check_run_id,
        "failure_message": None,
        "file_path": None,
        "outcome": CheckOutcome.SUCCESS,
        "severity": "error",
        "unique_id": unique_id,
    }


def test_fingerprint_ignores_index_and_message():
    """The fingerprint is stable across a changed index and message."""
    a = _failure("check_model_names:3:model.x", unique_id="model.x", message="was 5")
    b = _failure("check_model_names:9:model.x", unique_id="model.x", message="was 7")
    assert fingerprint(a) == fingerprint(b)


def test_fingerprint_uses_file_path_when_no_unique_id():
    """The fingerprint falls back to file_path when unique_id is absent."""
    result = _failure("check_x:0", file_path="models/a.sql")
    assert fingerprint(result) == "check_x::models/a.sql"


def test_fingerprint_distinguishes_checks_and_resources():
    """Different checks or resources produce different fingerprints."""
    base = _failure("check_a:0:model.x", unique_id="model.x")
    other_check = _failure("check_b:0:model.x", unique_id="model.x")
    other_resource = _failure("check_a:0:model.y", unique_id="model.y")
    assert fingerprint(base) != fingerprint(other_check)
    assert fingerprint(base) != fingerprint(other_resource)


def test_failure_fingerprints_only_failures():
    """Only failed results contribute a fingerprint."""
    results = [
        _failure("check_a:0:model.x", unique_id="model.x"),
        _success("check_a:1:model.y", unique_id="model.y"),
    ]
    assert failure_fingerprints(results) == {"check_a::model.x"}


def test_build_baseline_structure_and_sorted():
    """The baseline document is versioned and sorted by fingerprint."""
    results = [
        _failure("check_b:0:model.z", unique_id="model.z"),
        _failure("check_a:0:model.x", unique_id="model.x"),
        _success("check_a:1:model.y", unique_id="model.y"),
    ]
    document = build_baseline(results)
    assert document["version"] == 1
    fingerprints = [e["fingerprint"] for e in document["failures"]]
    assert fingerprints == sorted(fingerprints)
    assert len(document["failures"]) == 2


def test_load_baseline_round_trip(tmp_path):
    """A written baseline loads back to its fingerprints."""
    results = [_failure("check_a:0:model.x", unique_id="model.x")]
    path = tmp_path / "baseline.json"
    path.write_bytes(orjson.dumps(build_baseline(results)))
    assert load_baseline(path) == {"check_a::model.x"}


def test_load_baseline_missing_raises(tmp_path):
    """A missing baseline file raises a clear config error."""
    with pytest.raises(DbtBouncerConfigError, match="Could not read the baseline"):
        load_baseline(tmp_path / "does-not-exist.json")


def test_load_baseline_non_dict_raises(tmp_path):
    """A structurally valid JSON that is not an object raises a config error."""
    path = tmp_path / "baseline.json"
    path.write_bytes(orjson.dumps(["not", "a", "baseline"]))
    with pytest.raises(DbtBouncerConfigError, match="not a valid baseline"):
        load_baseline(path)


def test_load_baseline_version_mismatch_raises(tmp_path):
    """A baseline with an unexpected version raises a clear config error."""
    path = tmp_path / "baseline.json"
    path.write_bytes(orjson.dumps({"version": 999, "failures": []}))
    with pytest.raises(DbtBouncerConfigError, match="expects version"):
        load_baseline(path)


def test_load_baseline_skips_entry_without_fingerprint(tmp_path):
    """An entry missing a fingerprint is skipped rather than raising KeyError."""
    path = tmp_path / "baseline.json"
    path.write_bytes(
        orjson.dumps(
            {
                "version": 1,
                "failures": [{"check_name": "x"}, {"fingerprint": "check_a::model.x"}],
            }
        )
    )
    assert load_baseline(path) == {"check_a::model.x"}


def test_apply_regression_filter_suppresses_known():
    """A known failure is dropped and counted; a new failure is kept."""
    results = [
        _failure("check_a:0:model.known", unique_id="model.known"),
        _failure("check_a:1:model.new", unique_id="model.new"),
        _success("check_b:0:model.ok", unique_id="model.ok"),
    ]
    accepted = {"check_a::model.known"}
    kept, suppressed = apply_regression_filter(results, accepted)

    assert suppressed == 1
    kept_ids = {r["check_run_id"] for r in kept}
    assert kept_ids == {"check_a:1:model.new", "check_b:0:model.ok"}
