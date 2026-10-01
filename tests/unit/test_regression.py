"""Tests for the regression (baseline/state) filter."""

import tempfile
from pathlib import Path

import orjson
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from dbt_bouncer.enums import CheckOutcome, CheckSeverity
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


def _internal_error(check_run_id, unique_id=None):
    return {
        **_failure(check_run_id, unique_id=unique_id, message="crashed"),
        "outcome": CheckOutcome.INTERNAL_ERROR,
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


def test_internal_errors_are_never_baselined():
    """A crash is not a known failure, so it is left out of the baseline."""
    results = [
        _failure("check_a:0:model.one", unique_id="model.one"),
        _internal_error("check_b:0:model.two", unique_id="model.two"),
    ]

    assert failure_fingerprints(results) == {"check_a::model.one"}
    assert [e["fingerprint"] for e in build_baseline(results)["failures"]] == [
        "check_a::model.one"
    ]


def test_apply_regression_filter_keeps_internal_errors():
    """An internal error is reported even if its fingerprint is accepted."""
    results = [_internal_error("check_b:0:model.two", unique_id="model.two")]

    kept, suppressed = apply_regression_filter(results, {"check_b::model.two"})

    assert suppressed == 0
    assert kept == results


class TestRegressionProperties:
    """Property-based tests for baseline serialization and filtering."""

    @settings(max_examples=50)
    @given(
        st.lists(
            st.fixed_dictionaries(
                {
                    "check_run_id": st.tuples(
                        st.from_regex(r"check_[a-z_]+", fullmatch=True),
                        st.integers(min_value=0, max_value=100),
                    ).map(lambda p: f"{p[0]}:{p[1]}"),
                    "failure_message": st.text(),
                    "file_path": st.one_of(
                        st.none(),
                        st.from_regex(r"models/[a-z_]+\.sql", fullmatch=True),
                    ),
                    "outcome": st.sampled_from(
                        [
                            CheckOutcome.FAILED,
                            CheckOutcome.INTERNAL_ERROR,
                            CheckOutcome.SUCCESS,
                        ]
                    ),
                    "severity": st.sampled_from(list(CheckSeverity)),
                    "unique_id": st.one_of(
                        st.none(),
                        st.from_regex(
                            r"(model|source|seed)\.[a-z_]+\.[a-z_]+", fullmatch=True
                        ),
                    ),
                }
            ),
            max_size=20,
        )
    )
    def test_baseline_roundtrip_and_sorting_invariants(self, results):
        """build_baseline followed by load_baseline preserves failure fingerprints."""
        baseline_doc = build_baseline(results)
        assert baseline_doc["version"] == 1

        fingerprints = [entry["fingerprint"] for entry in baseline_doc["failures"]]
        assert fingerprints == sorted(fingerprints)

        with tempfile.TemporaryDirectory() as tmp_dir:
            baseline_file = Path(tmp_dir) / "baseline.json"
            baseline_file.write_bytes(orjson.dumps(baseline_doc))
            assert load_baseline(baseline_file) == failure_fingerprints(results)

    @settings(max_examples=50)
    @given(
        check_name=st.from_regex(r"check_[a-z_]+", fullmatch=True),
        unique_id=st.from_regex(r"model\.[a-z_]+\.[a-z_]+", fullmatch=True),
        idx1=st.integers(min_value=0, max_value=1000),
        idx2=st.integers(min_value=0, max_value=1000),
        msg1=st.text(),
        msg2=st.text(),
    )
    def test_fingerprint_invariance_property(
        self, check_name, unique_id, idx1, idx2, msg1, msg2
    ):
        """Fingerprint identity is invariant to volatile check run index and failure message."""
        res1 = _failure(f"{check_name}:{idx1}", unique_id=unique_id, message=msg1)
        res2 = _failure(f"{check_name}:{idx2}", unique_id=unique_id, message=msg2)
        assert fingerprint(res1) == fingerprint(res2) == f"{check_name}::{unique_id}"

    @settings(max_examples=50)
    @given(
        results=st.lists(
            st.fixed_dictionaries(
                {
                    "check_run_id": st.from_regex(
                        r"check_[a-z_]+:[0-9]+", fullmatch=True
                    ),
                    "failure_message": st.text(),
                    "file_path": st.one_of(
                        st.none(),
                        st.from_regex(r"models/[a-z_]+\.sql", fullmatch=True),
                    ),
                    "outcome": st.sampled_from(
                        [
                            CheckOutcome.FAILED,
                            CheckOutcome.INTERNAL_ERROR,
                            CheckOutcome.SUCCESS,
                        ]
                    ),
                    "severity": st.sampled_from(list(CheckSeverity)),
                    "unique_id": st.one_of(
                        st.none(),
                        st.from_regex(
                            r"(model|source|seed)\.[a-z_]+\.[a-z_]+", fullmatch=True
                        ),
                    ),
                }
            ),
            max_size=20,
        ),
        accepted=st.sets(
            st.from_regex(
                r"check_[a-z_]+::(model|source|seed)\.[a-z_]+\.[a-z_]+",
                fullmatch=True,
            ),
            max_size=10,
        ),
    )
    def test_apply_regression_filter_invariants(self, results, accepted):
        """Regression filter satisfies conservation, exclusion, safety, and idempotence laws."""
        kept, suppressed = apply_regression_filter(results, accepted)

        # 1. Conservation law: every result is either kept or counted as suppressed.
        assert len(kept) + suppressed == len(results)

        # 2. Strict suppression: no kept failure has an accepted fingerprint.
        for r in kept:
            if r.get("outcome") == CheckOutcome.FAILED:
                assert fingerprint(r) not in accepted

        # 3. Crash and success immunity: non-failed results are never suppressed.
        kept_non_failures = [r for r in kept if r.get("outcome") != CheckOutcome.FAILED]
        total_non_failures = [
            r for r in results if r.get("outcome") != CheckOutcome.FAILED
        ]
        assert len(kept_non_failures) == len(total_non_failures)

        # 4. Idempotence: re-filtering kept results suppresses nothing further.
        kept_again, suppressed_again = apply_regression_filter(kept, accepted)
        assert kept_again == kept
        assert suppressed_again == 0
