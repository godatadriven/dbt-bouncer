"""Protocol types for dbt artifact resources.

Defines structural interfaces that DictProxy objects satisfy via ``__getattr__``
at runtime, giving IDE autocomplete and type-checker support without importing
any concrete dbt-artifacts-parser classes.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

# ---------------------------------------------------------------------------
# Nested attribute protocols (not runtime-checkable)
# ---------------------------------------------------------------------------


class ColumnInfo(Protocol):
    """Column metadata from the dbt manifest or catalog."""

    data_type: str | None
    description: str
    index: int
    meta: dict[str, Any]
    name: str
    tags: list[str]
    type: str


class Constraint(Protocol):
    """Model-level constraint (e.g. primary_key, unique)."""

    columns: list[str]
    name: str | None
    type: str


class Contract(Protocol):
    """Model contract enforcement configuration."""

    enforced: bool


class DependsOn(Protocol):
    """Upstream dependencies of a node."""

    macros: list[str]
    nodes: list[str]


class Freshness(Protocol):
    """Source freshness thresholds."""

    error_after: FreshnessRule
    warn_after: FreshnessRule


class FreshnessRule(Protocol):
    """Single freshness threshold rule (warn or error)."""

    count: int | None
    period: str | None


class MacroArgument(Protocol):
    """Argument definition for a dbt macro."""

    description: str
    name: str


class ManifestMetadata(Protocol):
    """Top-level manifest metadata."""

    adapter_type: str
    dbt_version: str
    project_name: str


class NodeConfig(Protocol):
    """Node-level configuration block."""

    grants: dict[str, Any]
    materialized: str
    meta: dict[str, Any]
    tags: list[str]


class UnitTestExpect(Protocol):
    """Expected output of a unit test."""

    format: UnitTestFormat


class UnitTestFormat(Protocol):
    """Format specification for unit test input/output."""

    value: str


class UnitTestGiven(Protocol):
    """Input fixture for a unit test."""

    format: UnitTestFormat


# ---------------------------------------------------------------------------
# Top-level resource protocols (runtime-checkable)
# ---------------------------------------------------------------------------


@runtime_checkable
class CatalogNodeEntry(Protocol):
    """A node entry from the dbt catalog artifact."""

    columns: dict[str, ColumnInfo]
    unique_id: str


@runtime_checkable
class ExposureNode(Protocol):
    """A dbt exposure resource."""

    depends_on: DependsOn
    name: str
    package_name: str
    unique_id: str


@runtime_checkable
class MacroNode(Protocol):
    """A dbt macro resource."""

    arguments: list[MacroArgument]
    description: str
    macro_sql: str
    name: str
    original_file_path: str
    patch_path: str | None
    unique_id: str


@runtime_checkable
class ManifestObject(Protocol):
    """The top-level dbt manifest artifact."""

    child_map: dict[str, list[str]]
    metadata: ManifestMetadata
    nodes: dict[str, Any]


@runtime_checkable
class ModelNode(Protocol):
    """A dbt model resource."""

    access: Any
    columns: dict[str, ColumnInfo]
    config: NodeConfig
    constraints: list[Constraint]
    contract: Contract
    depends_on: DependsOn
    description: str
    latest_version: int | None
    meta: dict[str, Any]
    name: str
    original_file_path: str
    patch_path: str | None
    raw_code: str
    package_name: str
    resource_type: str
    schema_: str
    tags: list[str]
    unique_id: str
    version: int | None


@runtime_checkable
class RunResultEntry(Protocol):
    """A single entry from the dbt run results artifact."""

    adapter_response: dict[str, Any]
    execution_time: float
    status: str
    unique_id: str


@runtime_checkable
class SeedNode(Protocol):
    """A dbt seed resource."""

    columns: dict[str, ColumnInfo]
    description: str
    name: str
    unique_id: str


@runtime_checkable
class SemanticModelNode(Protocol):
    """A dbt semantic model resource."""

    depends_on: DependsOn
    name: str
    package_name: str
    unique_id: str


@runtime_checkable
class SnapshotNode(Protocol):
    """A dbt snapshot resource."""

    name: str
    tags: list[str]
    unique_id: str


@runtime_checkable
class SourceNode(Protocol):
    """A dbt source resource."""

    columns: dict[str, ColumnInfo]
    description: str
    freshness: Freshness
    loader: str
    meta: dict[str, Any]
    name: str
    original_file_path: str
    source_name: str
    tags: list[str]
    unique_id: str


@runtime_checkable
class TestNode(Protocol):
    """A dbt test resource."""

    depends_on: DependsOn
    tags: list[str]
    unique_id: str


@runtime_checkable
class UnitTestNode(Protocol):
    """A dbt unit test resource."""

    depends_on: DependsOn
    expect: UnitTestExpect
    given: list[UnitTestGiven]
    name: str
    unique_id: str


# ---------------------------------------------------------------------------
# Wrapper protocols for SimpleNamespace resource containers
# ---------------------------------------------------------------------------


@runtime_checkable
class CatalogNodeWrapper(Protocol):
    """Wrapper around a catalog node entry with path metadata."""

    catalog_node: CatalogNodeEntry
    original_file_path: str
    unique_id: str


@runtime_checkable
class CatalogSourceWrapper(Protocol):
    """Wrapper around a catalog source entry with path metadata."""

    catalog_source: CatalogNodeEntry
    original_file_path: str
    unique_id: str


@runtime_checkable
class ManifestWrapper(Protocol):
    """Wrapper around the parsed manifest artifact."""

    manifest: ManifestObject


@runtime_checkable
class ModelWrapper(Protocol):
    """Wrapper around a model node with path metadata."""

    model: ModelNode
    original_file_path: str
    unique_id: str


@runtime_checkable
class RunResultWrapper(Protocol):
    """Wrapper around a run result entry with path metadata."""

    original_file_path: str
    run_result: RunResultEntry
    unique_id: str


@runtime_checkable
class SeedWrapper(Protocol):
    """Wrapper around a seed node with path metadata."""

    original_file_path: str
    seed: SeedNode
    unique_id: str


@runtime_checkable
class SemanticModelWrapper(Protocol):
    """Wrapper around a semantic model node with path metadata."""

    original_file_path: str
    semantic_model: SemanticModelNode
    unique_id: str


@runtime_checkable
class SnapshotWrapper(Protocol):
    """Wrapper around a snapshot node with path metadata."""

    original_file_path: str
    snapshot: SnapshotNode
    unique_id: str


@runtime_checkable
class SourceWrapper(Protocol):
    """Wrapper around a source node with path metadata."""

    original_file_path: str
    source: SourceNode
    unique_id: str


@runtime_checkable
class TestWrapper(Protocol):
    """Wrapper around a test node with path metadata."""

    original_file_path: str
    test: TestNode
    unique_id: str
