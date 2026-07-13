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

    # Derived reverse-lookup indexes, computed once in __post_init__ from the
    # resource lists above. Not accepted as constructor args (init=False):
    # they must always be self-derived so they're correct whether CheckContext
    # is built via runner.py (production) or directly via testing.py (unit
    # tests), which never populate the lookup dicts above.
    children_by_unique_id: dict[str, list[Any]] = field(
        default_factory=dict, init=False, repr=False
    )
    tests_by_attached_node: dict[str, list[Any]] = field(
        default_factory=dict, init=False, repr=False
    )
    tests_by_depends_on_node: dict[str, list[Any]] = field(
        default_factory=dict, init=False, repr=False
    )
    unit_tests_by_depends_on_node: dict[str, list[Any]] = field(
        default_factory=dict, init=False, repr=False
    )
    sources_by_relation: dict[tuple[Any, Any, Any], list[str]] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        """Derive reverse-lookup indexes once from the resource lists.

        Storing actual objects (not just unique_ids) in children_by_unique_id
        means downstream checks never need a second models_by_unique_id
        lookup, which matters because that dict is frequently empty in test
        contexts (see the field comment above).
        """
        children: dict[str, list[Any]] = {}
        for m in self.models:
            nodes = getattr(getattr(m, "depends_on", None), "nodes", None)
            for upstream_id in set(nodes or []):
                children.setdefault(upstream_id, []).append(m)
        object.__setattr__(self, "children_by_unique_id", children)

        tests_by_attached: dict[str, list[Any]] = {}
        tests_by_dep: dict[str, list[Any]] = {}
        for t in self.tests:
            attached = getattr(t, "attached_node", None)
            if attached:
                tests_by_attached.setdefault(attached, []).append(t)
            dep_nodes = getattr(getattr(t, "depends_on", None), "nodes", None)
            for node_id in set(dep_nodes or []):
                tests_by_dep.setdefault(node_id, []).append(t)
        object.__setattr__(self, "tests_by_attached_node", tests_by_attached)
        object.__setattr__(self, "tests_by_depends_on_node", tests_by_dep)

        unit_tests_by_first_dep: dict[str, list[Any]] = {}
        for ut in self.unit_tests:
            dep_nodes = getattr(getattr(ut, "depends_on", None), "nodes", None)
            if dep_nodes:
                unit_tests_by_first_dep.setdefault(dep_nodes[0], []).append(ut)
        object.__setattr__(
            self, "unit_tests_by_depends_on_node", unit_tests_by_first_dep
        )

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
        object.__setattr__(self, "sources_by_relation", relations)
