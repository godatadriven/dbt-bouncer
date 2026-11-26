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

    # The dbt Information Schema (an ``InfoSchema``), loaded only when
    # ``info_schema_checks`` are configured.
    info_schema: Any = None

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

    # Derived reverse-lookup indexes, computed lazily on first access from the
    # resource lists above and cached on private slots. Lazy computation avoids
    # building indexes over thousands of models/tests when no enabled check
    # queries them, while staying self-derived whether CheckContext is built via
    # runner.py (production) or directly via testing.py (unit tests).
    _children_by_unique_id: dict[str, list[Any]] | None = field(
        default=None, init=False, repr=False
    )
    _tests_by_attached_node: dict[str, list[Any]] | None = field(
        default=None, init=False, repr=False
    )
    _tests_by_depends_on_node: dict[str, list[Any]] | None = field(
        default=None, init=False, repr=False
    )
    _unit_tests_by_depends_on_node: dict[str, list[Any]] | None = field(
        default=None, init=False, repr=False
    )
    _sources_by_relation: dict[tuple[Any, Any, Any], list[str]] | None = field(
        default=None, init=False, repr=False
    )

    @property
    def children_by_unique_id(self) -> dict[str, list[Any]]:
        """Map each upstream unique_id to its direct downstream model objects.

        Returns:
            dict[str, list[Any]]: Downstream models keyed by parent unique_id.

        """
        cached = self._children_by_unique_id
        if cached is not None:
            return cached
        children: dict[str, list[Any]] = {}
        for m in self.models:
            if isinstance(m, dict):
                dep = dict.get(m, "depends_on")
                nodes = dict.get(dep, "nodes") if isinstance(dep, dict) else None
            else:
                nodes = getattr(getattr(m, "depends_on", None), "nodes", None)
            if nodes:
                for upstream_id in set(nodes):
                    children.setdefault(upstream_id, []).append(m)
        object.__setattr__(self, "_children_by_unique_id", children)
        return children

    @property
    def tests_by_attached_node(self) -> dict[str, list[Any]]:
        """Map each node unique_id to the tests attached to it.

        Returns:
            dict[str, list[Any]]: Attached tests keyed by node unique_id.

        """
        cached = self._tests_by_attached_node
        if cached is not None:
            return cached
        tests_by_attached: dict[str, list[Any]] = {}
        for t in self.tests:
            attached = (
                dict.get(t, "attached_node")
                if isinstance(t, dict)
                else getattr(t, "attached_node", None)
            )
            if attached:
                tests_by_attached.setdefault(attached, []).append(t)
        object.__setattr__(self, "_tests_by_attached_node", tests_by_attached)
        return tests_by_attached

    @property
    def tests_by_depends_on_node(self) -> dict[str, list[Any]]:
        """Map each node unique_id to the tests that depend on it.

        Returns:
            dict[str, list[Any]]: Dependent tests keyed by node unique_id.

        """
        cached = self._tests_by_depends_on_node
        if cached is not None:
            return cached
        tests_by_dep: dict[str, list[Any]] = {}
        for t in self.tests:
            if isinstance(t, dict):
                dep = dict.get(t, "depends_on")
                dep_nodes = dict.get(dep, "nodes") if isinstance(dep, dict) else None
            else:
                dep_nodes = getattr(getattr(t, "depends_on", None), "nodes", None)
            if dep_nodes:
                for node_id in set(dep_nodes):
                    tests_by_dep.setdefault(node_id, []).append(t)
        object.__setattr__(self, "_tests_by_depends_on_node", tests_by_dep)
        return tests_by_dep

    @property
    def unit_tests_by_depends_on_node(self) -> dict[str, list[Any]]:
        """Map each node unique_id to unit tests whose first dependency is it.

        Returns:
            dict[str, list[Any]]: Unit tests keyed by first dependency unique_id.

        """
        cached = self._unit_tests_by_depends_on_node
        if cached is not None:
            return cached
        unit_tests_by_first_dep: dict[str, list[Any]] = {}
        for ut in self.unit_tests:
            if isinstance(ut, dict):
                dep = dict.get(ut, "depends_on")
                dep_nodes = dict.get(dep, "nodes") if isinstance(dep, dict) else None
            else:
                dep_nodes = getattr(getattr(ut, "depends_on", None), "nodes", None)
            if dep_nodes:
                unit_tests_by_first_dep.setdefault(dep_nodes[0], []).append(ut)
        object.__setattr__(
            self, "_unit_tests_by_depends_on_node", unit_tests_by_first_dep
        )
        return unit_tests_by_first_dep

    @property
    def sources_by_relation(self) -> dict[tuple[Any, Any, Any], list[str]]:
        """Map each ``(database, schema, identifier)`` tuple to source unique_ids.

        Returns:
            dict[tuple[Any, Any, Any], list[str]]: Source unique_ids per relation.

        """
        cached = self._sources_by_relation
        if cached is not None:
            return cached
        relations: dict[tuple[Any, Any, Any], list[str]] = {}
        for s in self.sources:
            # ctx.sources holds SourceWrapper objects (with a `.source`
            # attribute) in production but direct DictProxy objects in unit
            # tests - mirrors the _node() helper in
            # checks/manifest/sources/lineage.py::check_duplicate_sources.
            inner = getattr(s, "source", None)
            node = inner if inner is not None else s
            key = (node.database, node.schema, node.identifier)
            relations.setdefault(key, []).append(node.unique_id)
        object.__setattr__(self, "_sources_by_relation", relations)
        return relations
