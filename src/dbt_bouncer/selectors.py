"""dbt-style node selectors for check targeting.

Where ``include``/``exclude`` filter on file paths, a selector filters on
node properties and the dbt DAG. The supported syntax is a subset of dbt's
node selection:

- ``tag:finance`` — resources with the tag.
- ``path:models/staging`` — resources under a path (glob patterns supported).
- ``fqn:my_project.marts.*`` — glob match on the dot-joined fully
  qualified name.
- ``package:my_package`` — resources from a package.
- ``stg_customers`` / ``stg_*`` — glob match on the resource name.
- ``+orders`` — ``orders`` plus all its ancestors; ``orders+`` — plus all
  its descendants; ``+orders+`` — both. Graph operators can wrap any
  method (e.g. ``+tag:critical``).
- ``2+orders`` — ``orders`` plus ancestors up to 2 edges away; ``orders+3``
  — plus descendants up to 3 edges away. A degree limit truncates the graph
  walk at the given number of hops.
- Space-separated atoms are a union (OR); comma-separated atoms are an
  intersection (AND). Same semantics as dbt.

Not supported (use dbt itself for these): the ``@`` operator,
``state:``/``result:``/``config.*:``/``test_type:`` methods, and
YAML-defined named selectors.
"""

from __future__ import annotations

import re
from fnmatch import fnmatch
from typing import TYPE_CHECKING, Any

from dbt_bouncer.exceptions import DbtBouncerConfigError

if TYPE_CHECKING:
    from collections.abc import Iterator

_VALID_METHODS = ("fqn", "name", "package", "path", "tag")

# A leading ``+`` graph operator, optionally prefixed with a hop count
# (``2+orders``). An empty count means an unbounded walk.
_ANCESTOR_OP = re.compile(r"^(\d*)\+")
# A trailing ``+`` graph operator, optionally suffixed with a hop count
# (``orders+3``). An empty count means an unbounded walk.
_DESCENDANT_OP = re.compile(r"\+(\d*)$")

# Manifest collections that participate in selection, i.e. every collection
# whose members carry a ``unique_id`` that can appear in ``parent_map``/
# ``child_map`` or be iterated by a check.
_MANIFEST_COLLECTIONS = (
    "exposures",
    "macros",
    "nodes",
    "semantic_models",
    "sources",
    "unit_tests",
)


class SelectorAtom:
    """One parsed selector atom, e.g. ``tag:finance`` or ``+orders``."""

    __slots__ = (
        "ancestor_degree",
        "ancestors",
        "descendant_degree",
        "descendants",
        "method",
        "value",
    )

    def __init__(self, raw: str) -> None:
        """Parse a raw atom string.

        Args:
            raw: The atom text, optionally wrapped in graph operators
                (``+`` or ``2+``).

        Raises:
            DbtBouncerConfigError: If the atom is empty or uses an
                unsupported selection method.

        """
        self.ancestors = False
        self.ancestor_degree: int | None = None
        self.descendants = False
        self.descendant_degree: int | None = None
        core = raw
        ancestor = _ANCESTOR_OP.match(core)
        if ancestor:
            self.ancestors = True
            self.ancestor_degree = int(ancestor.group(1)) if ancestor.group(1) else None
            core = core[ancestor.end() :]
        descendant = _DESCENDANT_OP.search(core)
        if descendant:
            self.descendants = True
            self.descendant_degree = (
                int(descendant.group(1)) if descendant.group(1) else None
            )
            core = core[: descendant.start()]
        if not core:
            raise DbtBouncerConfigError(f"Invalid selector atom: '{raw}'.")
        if ":" in core:
            method, _, value = core.partition(":")
            if method not in _VALID_METHODS or not value:
                raise DbtBouncerConfigError(
                    f"Invalid selector atom: '{raw}'. Supported methods: {', '.join(_VALID_METHODS)}."
                )
            self.method, self.value = method, value
        else:
            self.method, self.value = "name", core

    def matches_node(self, unique_id: str, node: Any) -> bool:
        """Whether a manifest resource matches this atom, ignoring graph operators.

        Args:
            unique_id: The resource's unique ID.
            node: The manifest resource object.

        Returns:
            bool: True when the resource matches.

        """
        if self.method == "name":
            # Fall back to the last unique-ID segment for resources whose
            # manifest entry carries no ``name`` attribute (e.g. proxy objects
            # for exotic resource types) — intentional, so name atoms still
            # work everywhere.
            name = getattr(node, "name", None) or unique_id.split(".")[-1]
            return fnmatch(str(name), self.value)
        if self.method == "tag":
            tags = getattr(node, "tags", None) or []
            return self.value in [str(t) for t in tags]
        if self.method == "package":
            return str(getattr(node, "package_name", "")) == self.value
        if self.method == "path":
            path = str(getattr(node, "original_file_path", "")).replace("\\", "/")
            value = self.value.rstrip("/")
            if any(c in value for c in "*?["):
                return fnmatch(path, value) or fnmatch(path, f"{value}/*")
            return path == value or path.startswith(f"{value}/")
        # self.method == "fqn"
        fqn = getattr(node, "fqn", None)
        if not fqn:
            return False
        return fnmatch(".".join(str(part) for part in fqn), self.value)


def parse_selector(raw: str) -> list[list[SelectorAtom]]:
    """Parse a selector string into a union of intersections of atoms.

    Space-separated atoms form a union; comma-separated atoms within one
    token form an intersection.

    Args:
        raw: The selector string.

    Returns:
        list[list[SelectorAtom]]: The parsed union of intersection groups.

    Raises:
        DbtBouncerConfigError: If the selector is empty or contains an
            invalid atom.

    """
    groups = [
        [SelectorAtom(atom) for atom in token.split(",") if atom != ""]
        for token in raw.split()
    ]
    groups = [group for group in groups if group]
    if not groups:
        raise DbtBouncerConfigError(f"Invalid selector: '{raw}'.")
    return groups


class Selector:
    """A parsed selector resolved against one manifest.

    Resolution happens once, up front: each atom is evaluated against every
    selectable manifest resource, graph operators expand the result over
    ``parent_map``/``child_map``, and the union/intersection structure
    reduces the atom sets to a single set of unique IDs.
    """

    def __init__(self, raw: str, manifest: Any) -> None:
        """Parse ``raw`` and resolve it against ``manifest``.

        Args:
            raw: The selector string.
            manifest: The parsed ``manifest.json`` object (must expose the
                resource collections plus ``parent_map``/``child_map``).

        """
        self.raw = raw
        groups = parse_selector(raw)

        resources = list(self._iter_manifest_resources(manifest))
        parent_map = getattr(manifest, "parent_map", None) or {}
        child_map = getattr(manifest, "child_map", None) or {}

        selected: set[str] = set()
        for group in groups:
            group_ids: set[str] | None = None
            for atom in group:
                atom_ids = {
                    uid for uid, node in resources if atom.matches_node(uid, node)
                }
                # Walk both directions from the original matched seed so that
                # ``+x+`` does not treat ancestors as new seeds for the
                # descendant walk.
                expanded = set(atom_ids)
                if atom.ancestors:
                    expanded |= self._closure(
                        atom_ids, parent_map, atom.ancestor_degree
                    )
                if atom.descendants:
                    expanded |= self._closure(
                        atom_ids, child_map, atom.descendant_degree
                    )
                atom_ids = expanded
                group_ids = atom_ids if group_ids is None else group_ids & atom_ids
            selected |= group_ids or set()
        self._selected_ids = selected

    @staticmethod
    def _iter_manifest_resources(manifest: Any) -> Iterator[tuple[str, Any]]:
        """Yield ``(unique_id, resource)`` pairs from all manifest collections.

        Yields:
            tuple[str, Any]: The unique ID and resource object.

        """
        for attr in _MANIFEST_COLLECTIONS:
            collection = getattr(manifest, attr, None)
            if collection is None or not hasattr(collection, "items"):
                continue
            for uid, node in collection.items():
                yield str(uid), node

    @staticmethod
    def _closure(
        seed_ids: set[str], edge_map: Any, degree: int | None = None
    ) -> set[str]:
        """Return the transitive closure of ``seed_ids`` over ``edge_map``.

        The walk runs level by level so that ``degree`` can cap it at a fixed
        number of hops from the seeds.

        Args:
            seed_ids: The starting unique IDs.
            edge_map: ``parent_map`` (for ancestors) or ``child_map`` (for
                descendants).
            degree: The maximum number of hops to walk, or None for an
                unbounded walk.

        Returns:
            set[str]: Every unique ID reached within ``degree`` hops of the
            seeds, excluding the seeds themselves.

        """
        reached: set[str] = set()
        frontier = set(seed_ids)
        hops = 0
        while frontier and (degree is None or hops < degree):
            hops += 1
            next_frontier: set[str] = set()
            for uid in frontier:
                try:
                    neighbours = edge_map[uid]
                except (KeyError, TypeError):
                    continue
                for neighbour in neighbours or []:
                    n = str(neighbour)
                    if n not in reached and n not in seed_ids:
                        reached.add(n)
                        next_frontier.add(n)
            frontier = next_frontier
        return reached

    def matches(self, unique_id: str | None) -> bool:
        """Whether a resource is selected.

        Args:
            unique_id: The resource's unique ID, or None when the resource
                has no identity in the manifest (never selected).

        Returns:
            bool: True when the resource is selected.

        """
        return unique_id is not None and str(unique_id) in self._selected_ids
