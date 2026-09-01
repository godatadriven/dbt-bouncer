"""Regression filtering for baseline and state runs.

A baseline is the set of failures a project already has. When a baseline is
applied, dbt-bouncer reports only failures that are not in the baseline, so a
team can adopt strict checks and fail only on newly introduced problems.

The same filter powers `--state`, where the accepted set comes from running the
checks against a directory of artifacts from a previous run instead of a stored
file.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from dbt_bouncer.enums import CheckOutcome

if TYPE_CHECKING:
    from pathlib import Path

BASELINE_VERSION = 1


def fingerprint(result: dict[str, Any]) -> str:
    """Build a stable identity for a single check result.

    The identity is the check name plus the resource it ran against. It does
    not include the failure message (messages carry volatile values such as
    counts) or the per-run check index (indices shift when the config is
    reordered). This keeps a baseline stable across unrelated changes.

    Args:
        result: A check result dict with `check_run_id`, `unique_id`, and
            `file_path`.

    Returns:
        str: The fingerprint string.

    """
    # `check_run_id` has the form "check_name:index[:resource_suffix]"; the check
    # name is the first colon-separated field.
    check_run_id = str(result.get("check_run_id", ""))
    check_name = check_run_id.split(":", 1)[0]
    resource = result.get("unique_id") or result.get("file_path") or ""
    return f"{check_name}::{resource}"


def failure_fingerprints(results: list[dict[str, Any]]) -> set[str]:
    """Return the fingerprints of the failed results.

    Args:
        results: The full list of check result dicts.

    Returns:
        set[str]: One fingerprint per failed result.

    """
    return {fingerprint(r) for r in results if r.get("outcome") == CheckOutcome.FAILED}


def build_baseline(results: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a baseline document from a set of check results.

    Args:
        results: The full list of check result dicts.

    Returns:
        dict[str, Any]: A JSON-serialisable baseline document.

    """
    failures = [r for r in results if r.get("outcome") == CheckOutcome.FAILED]
    entries = sorted(
        (
            {
                "check_name": str(r.get("check_run_id", "")).split(":", 1)[0],
                "file_path": r.get("file_path"),
                "fingerprint": fingerprint(r),
                "unique_id": r.get("unique_id"),
            }
            for r in failures
        ),
        key=lambda e: e["fingerprint"],
    )
    return {"version": BASELINE_VERSION, "failures": entries}


def load_baseline(path: Path) -> set[str]:
    """Load the accepted fingerprints from a baseline file.

    Args:
        path: Path to a baseline JSON file written by `dbt-bouncer baseline`.

    Returns:
        set[str]: The fingerprints of the accepted failures.

    Raises:
        DbtBouncerConfigError: If the file is missing, cannot be parsed, or has an
            unexpected version.

    """
    import orjson

    from dbt_bouncer.exceptions import DbtBouncerConfigError

    try:
        document = orjson.loads(path.read_bytes())
    except (FileNotFoundError, orjson.JSONDecodeError) as e:
        raise DbtBouncerConfigError(
            f"Could not read the baseline file `{path}`: {e}. Generate one with `dbt-bouncer baseline`."
        ) from e

    if not isinstance(document, dict):
        raise DbtBouncerConfigError(
            f"Baseline file `{path}` is not a valid baseline document. Regenerate it with `dbt-bouncer baseline`."
        )

    version = document.get("version")
    if version != BASELINE_VERSION:
        raise DbtBouncerConfigError(
            f"Baseline file `{path}` has version `{version}`, but this dbt-bouncer expects version `{BASELINE_VERSION}`. Regenerate it with `dbt-bouncer baseline`."
        )

    # Skip entries missing a fingerprint so a hand-edited file fails safe rather
    # than raising a KeyError.
    return {
        fingerprint_value
        for entry in document.get("failures", [])
        if (fingerprint_value := entry.get("fingerprint"))
    }


def apply_regression_filter(
    results: list[dict[str, Any]], accepted: set[str]
) -> tuple[list[dict[str, Any]], int]:
    """Drop failures whose fingerprint is in the accepted set.

    Passed results are kept unchanged. A failure is suppressed only when its
    fingerprint is accepted, so newly introduced failures still report.

    Args:
        results: The full list of check result dicts.
        accepted: The fingerprints to treat as already known.

    Returns:
        tuple[list[dict[str, Any]], int]: The kept results and the number of
            suppressed failures.

    """
    kept: list[dict[str, Any]] = []
    suppressed = 0
    for r in results:
        if r.get("outcome") == CheckOutcome.FAILED and fingerprint(r) in accepted:
            suppressed += 1
            continue
        kept.append(r)

    if suppressed:
        logging.info(
            f"Suppressed {suppressed} known failure(s) via the baseline/state comparison."
        )
    return kept, suppressed
