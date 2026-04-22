"""Typed context object for check execution."""

from dataclasses import dataclass, field
from typing import Any

__all__ = ["CheckContext"]


@dataclass(frozen=True, slots=True)
class CheckContext:
    """Immutable context holding all parsed dbt artifacts for check execution.

    Passed to checks via check._ctx so checks access artifacts explicitly
    rather than via magically injected attributes.

    All list fields contain **unwrapped** inner objects (e.g. DbtBouncerModelBase,
    not DbtBouncerModel wrappers), matching the current parsed_data dict in runner.py.
    """

    manifest_obj: Any = None

    # Resource lists (unwrapped inner objects)
    catalog_nodes: list[Any] = field(default_factory=list)
    catalog_sources: list[Any] = field(default_factory=list)
    exposures: list[Any] = field(default_factory=list)
    macros: list[Any] = field(default_factory=list)
    models: list[Any] = field(default_factory=list)
    run_results: list[Any] = field(default_factory=list)
    seeds: list[Any] = field(default_factory=list)
    semantic_models: list[Any] = field(default_factory=list)
    snapshots: list[Any] = field(default_factory=list)
    sources: list[Any] = field(default_factory=list)
    tests: list[Any] = field(default_factory=list)
    unit_tests: list[Any] = field(default_factory=list)

    # Lookup dicts
    exposures_by_unique_id: dict[str, Any] = field(default_factory=dict)
    models_by_unique_id: dict[str, Any] = field(default_factory=dict)
    sources_by_unique_id: dict[str, Any] = field(default_factory=dict)
    tests_by_unique_id: dict[str, Any] = field(default_factory=dict)
