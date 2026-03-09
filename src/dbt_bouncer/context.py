"""A context object to hold all the data needed for a bouncer run."""

from __future__ import annotations

from functools import cached_property
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict


class BouncerContext(BaseModel):
    """A context object to hold all the data needed for a bouncer run."""

    model_config = ConfigDict(arbitrary_types_allowed=True, ignored_types=(cached_property,))
    bouncer_config: Any
    catalog_nodes: list[Any]
    catalog_sources: list[Any]
    check_categories: list[str]
    create_pr_comment_file: bool
    dry_run: bool
    exposures: list[Any]
    macros: list[Any]
    manifest_obj: Any
    models: list[Any]
    output_file: Path | None
    output_format: str
    output_only_failures: bool
    run_results: list[Any]
    seeds: list[Any]
    semantic_models: list[Any]
    show_all_failures: bool
    snapshots: list[Any]
    sources: list[Any]
    tests: list[Any]
    unit_tests: list[Any]

    @cached_property
    def exposures_by_unique_id(self) -> dict[str, Any]:
        """Return a dict of exposure objects keyed by unique_id."""
        return {e.unique_id: e for e in self.exposures}

    @cached_property
    def models_flat(self) -> list[Any]:
        """Return a list of unwrapped model objects."""
        return [m.model for m in self.models]

    @cached_property
    def models_by_unique_id(self) -> dict[str, Any]:
        """Return a dict of unwrapped model objects keyed by unique_id."""
        return {m.model.unique_id: m.model for m in self.models}

    @cached_property
    def run_results_flat(self) -> list[Any]:
        """Return a list of unwrapped run result objects."""
        return [r.run_result for r in self.run_results]

    @cached_property
    def seeds_flat(self) -> list[Any]:
        """Return a list of unwrapped seed objects."""
        return [s.seed for s in self.seeds]

    @cached_property
    def semantic_models_flat(self) -> list[Any]:
        """Return a list of unwrapped semantic model objects."""
        return [sm.semantic_model for sm in self.semantic_models]

    @cached_property
    def snapshots_flat(self) -> list[Any]:
        """Return a list of unwrapped snapshot objects."""
        return [s.snapshot for s in self.snapshots]

    @cached_property
    def sources_by_unique_id(self) -> dict[str, Any]:
        """Return a dict of unwrapped source objects keyed by unique_id."""
        return {s.source.unique_id: s.source for s in self.sources}

    @cached_property
    def tests_by_unique_id(self) -> dict[str, Any]:
        """Return a dict of unwrapped test objects keyed by unique_id."""
        return {t.test.unique_id: t.test for t in self.tests}

    @cached_property
    def tests_flat(self) -> list[Any]:
        """Return a list of unwrapped test objects."""
        return [t.test for t in self.tests]
