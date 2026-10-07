"""Reader for the dbt Information Schema (dbt 2.0+).

dbt writes the Information Schema as Parquet files under
``<target>/info_schema/v1/`` when a command runs with ``--generate-info-schema``.
dbt documents this directory as a contracted, versioned interface, so dbt-bouncer
reads it rather than the undocumented ``<target>/private/`` index. See
https://docs.getdbt.com/reference/info-schema.

``duckdb`` is imported lazily: only runs that configure ``info_schema_checks``
pay for the import.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any

from dbt_bouncer.exceptions import DbtBouncerArtifactError

if TYPE_CHECKING:
    from pathlib import Path

    import duckdb

__all__ = [
    "INFO_SCHEMA_VERSION",
    "VALUE_LINEAGE_KINDS",
    "InfoSchema",
    "LineageEdge",
    "info_schema_dir",
]

# The only Information Schema version dbt publishes so far. The directory name
# (`v1`) and `dbt.project.schema_version` both carry it.
INFO_SCHEMA_VERSION = 1

# `copy` and `mod` edges carry a parent column's value into the child column.
# `scan` edges only mean the parent column was read (e.g. in a join or filter).
VALUE_LINEAGE_KINDS = frozenset({"copy", "mod"})

# Parquet files are named `<schema>.<table>.parquet`, e.g. `dbt.models.parquet`.
_TABLE_FILE_PATTERN = re.compile(r"^([a-z_][a-z0-9_]*)\.([a-z_][a-z0-9_]*)$")


def info_schema_dir(dbt_artifacts_dir: Path) -> Path:
    """Return the versioned Information Schema directory for a dbt target directory.

    Returns:
        Path: ``<dbt_artifacts_dir>/info_schema/v1``.

    """
    return dbt_artifacts_dir / "info_schema" / f"v{INFO_SCHEMA_VERSION}"


@dataclass(frozen=True, slots=True)
class LineageEdge:
    """One column-level lineage edge from ``dbt.column_lineage``."""

    parent_node_unique_id: str
    parent_column_name: str
    child_node_unique_id: str
    child_column_name: str
    evolution: str


class InfoSchema:
    """Tables from the dbt Information Schema, plus lookups the checks share.

    Build one with :meth:`from_directory` (production) or :meth:`from_rows`
    (unit tests). Every table is a list of row dicts keyed by column name.
    Column names are matched case-insensitively, because warehouses such as
    Snowflake upper-case them in some tables but not others.
    """

    def __init__(
        self,
        tables: dict[str, list[dict[str, Any]]] | None = None,
        directory: Path | None = None,
    ) -> None:
        """Store pre-loaded tables or the directory to load them from lazily."""
        self._tables: dict[str, list[dict[str, Any]]] = dict(tables or {})
        self.directory = directory

    @classmethod
    def from_directory(cls, directory: Path) -> InfoSchema:
        """Open an Information Schema directory and confirm its version.

        Args:
            directory: The ``info_schema/v1`` directory.

        Returns:
            InfoSchema: A reader backed by ``directory``.

        Raises:
            DbtBouncerArtifactError: If the directory is missing or reports an
                unsupported ``schema_version``.

        """
        if not (directory / "dbt.project.parquet").exists():
            raise DbtBouncerArtifactError(
                f"No dbt Information Schema found at `{directory}`. It requires dbt 2.0 or later: "
                "run `dbt build --generate-info-schema` (or `dbt compile --generate-info-schema`)."
            )
        info_schema = cls(directory=directory)
        project_rows = info_schema.table("dbt.project")
        versions = {row.get("schema_version") for row in project_rows}
        if versions != {INFO_SCHEMA_VERSION}:
            raise DbtBouncerArtifactError(
                f"`{directory}` has Information Schema version {sorted(versions, key=str)}, "
                f"dbt-bouncer supports version {INFO_SCHEMA_VERSION} only."
            )
        return info_schema

    @classmethod
    def from_rows(cls, **tables: list[dict[str, Any]]) -> InfoSchema:
        """Build an in-memory Information Schema, for unit tests.

        Pass each table as a keyword argument named after the table without its
        ``dbt.`` schema, e.g. ``from_rows(column_lineage=[...], models=[...])``.

        Returns:
            InfoSchema: A reader over the given rows.

        """
        return cls(tables={f"dbt.{name}": rows for name, rows in tables.items()})

    @cached_property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """An in-memory DuckDB database holding every Information Schema table.

        Each ``<schema>.<table>.parquet`` file becomes the table
        ``<schema>.<table>``. After loading, external access is disabled and the
        configuration locked, so SQL from a config file cannot read or write files.

        Returns:
            duckdb.DuckDBPyConnection: The connection.

        Raises:
            DbtBouncerArtifactError: If the reader has no backing directory.

        """
        if self.directory is None:
            raise DbtBouncerArtifactError(
                "This Information Schema was built from in-memory rows and has no database."
            )
        import duckdb

        con = duckdb.connect(":memory:")
        for parquet_file in sorted(self.directory.glob("*.parquet")):
            match = _TABLE_FILE_PATTERN.match(parquet_file.stem)
            if match is None:
                continue
            schema, table = match.groups()
            con.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
            # Identifiers come from the allow-list regex above; the path is a bound parameter.
            con.execute(
                f'CREATE TABLE "{schema}"."{table}" AS SELECT * FROM read_parquet(?)',  # ruff: ignore[hardcoded-sql-expression] # nosec B608
                [parquet_file.as_posix()],
            )
        con.execute("SET enable_external_access = false")
        con.execute("SET lock_configuration = true")
        return con

    def query(self, sql: str) -> tuple[list[str], list[tuple[str | None, ...]]]:
        """Run a read-only SELECT against the Information Schema.

        Every value comes back as text, so results print as-is and time zone
        aware timestamps need no extra Python packages.

        Args:
            sql: A single SELECT statement.

        Returns:
            tuple[list[str], list[tuple[str | None, ...]]]: Column names and rows.

        """
        cursor = self.connection.execute(
            f"SELECT CAST(COLUMNS(*) AS VARCHAR) FROM ({sql.strip().rstrip(';')})"  # ruff: ignore[hardcoded-sql-expression] # nosec B608
        )
        columns = [d[0] for d in cursor.description or []]
        return columns, cursor.fetchall()

    def table(self, name: str) -> list[dict[str, Any]]:
        """Return every row of a table, e.g. ``table("dbt.models")``.

        A table that the directory does not contain returns no rows.

        Returns:
            list[dict[str, Any]]: The rows.

        """
        if name not in self._tables:
            rows: list[dict[str, Any]] = []
            if self.directory is not None:
                schema, table = name.split(".", 1)
                exists = self.connection.execute(
                    "SELECT 1 FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
                    [schema, table],
                ).fetchone()
                if exists:
                    rows = self._read_table(schema, table)
            self._tables[name] = rows
        return self._tables[name]

    def _read_table(self, schema: str, table: str) -> list[dict[str, Any]]:
        """Read a table into row dicts, keeping list columns (e.g. ``grain``) as lists.

        Time zone aware timestamps are cast to text: converting them to Python
        objects requires the ``pytz`` package.

        Returns:
            list[dict[str, Any]]: The rows.

        """
        column_types = self.connection.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position",
            [schema, table],
        ).fetchall()
        select_list = ", ".join(
            f'CAST("{name}" AS VARCHAR) AS "{name}"'
            if data_type == "TIMESTAMP WITH TIME ZONE"
            else f'"{name}"'
            for name, data_type in column_types
        )
        cursor = self.connection.execute(
            f'SELECT {select_list} FROM "{schema}"."{table}"'  # ruff: ignore[hardcoded-sql-expression] # nosec B608
        )
        columns = [d[0] for d in cursor.description or []]
        return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]

    @cached_property
    def lineage_edges(self) -> list[LineageEdge]:
        """Every column-level lineage edge.

        Returns:
            list[LineageEdge]: The edges.

        """
        return [
            LineageEdge(
                parent_node_unique_id=row["parent_node_unique_id"],
                parent_column_name=row["parent_column_name"],
                child_node_unique_id=row["child_node_unique_id"],
                child_column_name=row["child_column_name"],
                evolution=row["evolution"],
            )
            for row in self.table("dbt.column_lineage")
        ]

    @cached_property
    def lineage_by_child(self) -> dict[str, dict[str, list[LineageEdge]]]:
        """Incoming lineage edges keyed by child node unique_id, then lower-cased child column.

        Returns:
            dict[str, dict[str, list[LineageEdge]]]: The edges per child column.

        """
        by_child: dict[str, dict[str, list[LineageEdge]]] = {}
        for edge in self.lineage_edges:
            by_child.setdefault(edge.child_node_unique_id, {}).setdefault(
                edge.child_column_name.casefold(), []
            ).append(edge)
        return by_child

    @cached_property
    def lineage_parent_columns(self) -> dict[str, set[str]]:
        """Lower-cased column names with at least one downstream edge, per node.

        Returns:
            dict[str, set[str]]: Node unique_id to its consumed column names.

        """
        consumed: dict[str, set[str]] = {}
        for edge in self.lineage_edges:
            consumed.setdefault(edge.parent_node_unique_id, set()).add(
                edge.parent_column_name.casefold()
            )
        return consumed

    @cached_property
    def models_by_unique_id(self) -> dict[str, dict[str, Any]]:
        """Rows of ``dbt.models`` keyed by unique_id.

        Returns:
            dict[str, dict[str, Any]]: The model rows.

        """
        return {row["unique_id"]: row for row in self.table("dbt.models")}

    @cached_property
    def node_columns(self) -> dict[str, dict[str, dict[str, Any]]]:
        """Rows of ``dbt.node_columns`` keyed by node unique_id, then lower-cased column name.

        Returns:
            dict[str, dict[str, dict[str, Any]]]: The column rows per node.

        """
        by_node: dict[str, dict[str, dict[str, Any]]] = {}
        for row in self.table("dbt.node_columns"):
            by_node.setdefault(row["node_unique_id"], {})[
                row["column_name"].casefold()
            ] = row
        return by_node
