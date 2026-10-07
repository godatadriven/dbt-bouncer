"""Column checks that use the dbt Information Schema (column types and column-level lineage)."""

import re
from typing import TYPE_CHECKING, Any

from dbt_bouncer.artifact_parsers.info_schema import VALUE_LINEAGE_KINDS
from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.types import RegexPattern
from dbt_bouncer.utils import get_clean_model_name

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.info_schema import InfoSchema, LineageEdge

# Type families, matched against lower-cased type names. dbt reports declared
# types as written in YAML (`bigint`), and inferred types as Apache Arrow names
# (`Int64`, see dbt-labs/dbt#16515), so both spellings appear here. Types that
# match no family (e.g. arrays, structs) are not compared.
_TYPE_FAMILIES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("boolean", re.compile(r"^bool(ean)?$")),
    (
        "date",
        re.compile(r"^date(32|64)?$"),
    ),
    (
        "decimal",
        re.compile(r"^(big)?(decimal|numeric|number)(128|256)?(\s*\(.*\))?$"),
    ),
    (
        "float",
        re.compile(r"^(float(2|4|8|16|32|64)?|double( precision)?|real)$"),
    ),
    (
        "integer",
        re.compile(
            r"^(u?int(2|4|8|16|32|64|128)?|integer|u?bigint|u?smallint|u?tinyint|u?hugeint|uinteger|byteint)$"
        ),
    ),
    (
        "string",
        re.compile(
            r"^(large)?(utf8(view)?|string|text|varchar|char|character( varying)?|nvarchar|nchar|bpchar)(\s*\(.*\))?$"
        ),
    ),
    ("timestamp", re.compile(r"^(timestamp|datetime)")),
)


def _type_family(data_type: str | None) -> str | None:
    """Return the family of a column type, or `None` when the type is unknown.

    Returns:
        str | None: One of the `_TYPE_FAMILIES` names.

    """
    if not data_type:
        return None
    normalised = data_type.strip().casefold()
    for family, pattern in _TYPE_FAMILIES:
        if pattern.match(normalised):
            return family
    return None


def _manifest_column_meta(ctx: Any, unique_id: str, column_name: str) -> dict[str, Any]:
    """Return a column's `meta` from `manifest.json`.

    `dbt.node_columns.meta` is always empty in the Information Schema, so the
    manifest stays the source of truth for column meta.

    Returns:
        dict[str, Any]: The column meta, empty when the column is not declared.

    """
    manifest = ctx.manifest_obj.manifest
    node = (manifest.nodes or {}).get(unique_id) or (manifest.sources or {}).get(
        unique_id
    )
    if node is None:
        return {}
    for name, column in (node.columns or {}).items():
        if name.casefold() == column_name.casefold():
            config_meta = column.config.meta if column.config else None
            return dict(config_meta or column.meta or {})
    return {}


def _has_meta_key(ctx: Any, unique_id: str, column_name: str, meta_key: str) -> bool:
    """Whether a column's meta sets `meta_key` to a truthy value.

    Returns:
        bool: `True` when the key is set and truthy.

    """
    return bool(_manifest_column_meta(ctx, unique_id, column_name).get(meta_key))


def _value_edges(edges: "list[LineageEdge]") -> "list[LineageEdge]":
    """Keep the edges that carry a parent value into the child column.

    Returns:
        list[LineageEdge]: The `copy` and `mod` edges.

    """
    return [e for e in edges if e.evolution in VALUE_LINEAGE_KINDS]


def _column_name(info_schema: "InfoSchema", unique_id: str, key: str) -> str:
    """Return a column's name as dbt reports it, from its lower-cased key.

    Returns:
        str: The column name.

    """
    row = info_schema.node_columns.get(unique_id, {}).get(key)
    return row["column_name"] if row else key


@check(code="IS002")
def check_model_column_descriptions_propagated(model, ctx):
    """Columns that copy an upstream column must have a description when the upstream column has one.

    !!! info "Rationale"

        A column that is passed through unchanged from an upstream model or source means the same thing downstream. When the upstream column is documented but the downstream column is not, the description is lost one step later in the DAG and users of the downstream model see an undocumented column. This check uses dbt's column-level lineage to find these columns, so you can copy the description or reference a shared `doc` block.

    !!! note

        This check requires the dbt Information Schema (dbt 2.0+, `--generate-info-schema`). Only `copy` lineage edges are followed: a column that transforms its input (`mod`) can mean something different and is not checked.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        info_schema_checks:
            - name: check_model_column_descriptions_propagated
              include: ^models/marts
        ```

    """
    info_schema: "InfoSchema" = ctx.info_schema
    columns = info_schema.node_columns.get(model.unique_id, {})
    undocumented: list[str] = []
    for key, edges in sorted(
        info_schema.lineage_by_child.get(model.unique_id, {}).items()
    ):
        if (columns.get(key, {}).get("description") or "").strip():
            continue
        for edge in edges:
            if edge.evolution != "copy":
                continue
            parent = info_schema.node_columns.get(edge.parent_node_unique_id, {}).get(
                edge.parent_column_name.casefold(), {}
            )
            if (parent.get("description") or "").strip():
                undocumented.append(
                    f"`{_column_name(info_schema, model.unique_id, key)}` (from `{edge.parent_node_unique_id}.{edge.parent_column_name}`)"
                )
                break
    if undocumented:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has undocumented columns whose upstream column is documented: {', '.join(undocumented)}."
        )


@check(code="IS003")
def check_model_column_meta_propagated(model, ctx, *, meta_key: str):
    """Columns derived from an upstream column that sets a `meta` key must set the same key.

    !!! info "Rationale"

        Column `meta` often classifies data, for example `pii: true` or `contains_financial_data: true`. When a classified column flows into a downstream model, the downstream column holds the same data, but nothing in dbt copies the classification. Masking policies, access reviews and data catalogs that read the `meta` key then miss the downstream column. This check uses dbt's column-level lineage to make sure the classification follows the data.

    !!! note

        This check requires the dbt Information Schema (dbt 2.0+, `--generate-info-schema`). It follows `copy` and `mod` lineage edges (the column value flows downstream), not `scan` edges (the column is only read, e.g. in a join). Column `meta` is read from `manifest.json`.

    Parameters:
        meta_key (str): The `meta` key to propagate, e.g. `pii`. A column is classified when the key has a truthy value.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        info_schema_checks:
            - name: check_model_column_meta_propagated
              meta_key: pii
        ```

    """
    info_schema: "InfoSchema" = ctx.info_schema
    missing: list[str] = []
    for key, edges in sorted(
        info_schema.lineage_by_child.get(model.unique_id, {}).items()
    ):
        column_name = _column_name(info_schema, model.unique_id, key)
        if _has_meta_key(ctx, model.unique_id, column_name, meta_key):
            continue
        for edge in _value_edges(edges):
            if _has_meta_key(
                ctx, edge.parent_node_unique_id, edge.parent_column_name, meta_key
            ):
                missing.append(
                    f"`{column_name}` (from `{edge.parent_node_unique_id}.{edge.parent_column_name}`)"
                )
                break
    if missing:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has columns derived from a column with `meta.{meta_key}` that do not set it: {', '.join(missing)}."
        )


@check(code="IS004")
def check_model_column_types_match_inferred(model, ctx):
    """The declared `data_type` of a column must match the type that the model SQL produces.

    !!! info "Rationale"

        The `data_type` declared in YAML is what consumers rely on, and with an enforced contract dbt casts the column to that type. When the SQL produces a different type, for example an integer where a `double` is declared, the declaration hides a modelling mistake or a silent cast. dbt's static analysis infers the type that the SQL produces, so this check can compare the two without running the model.

    !!! note

        This check requires the dbt Information Schema (dbt 2.0+, `--generate-info-schema`). Types are compared by family (boolean, date, decimal, float, integer, string, timestamp), so `bigint` and `Int64` match while `double` and `Int64` do not. Columns without both a declared and an inferred type, or with a type outside these families, are not checked.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        info_schema_checks:
            - name: check_model_column_types_match_inferred
        ```

    """
    info_schema: "InfoSchema" = ctx.info_schema
    mismatches: list[str] = []
    for _, row in sorted(info_schema.node_columns.get(model.unique_id, {}).items()):
        declared = row.get("data_type_declared")
        inferred = row.get("data_type_inferred")
        declared_family = _type_family(declared)
        inferred_family = _type_family(inferred)
        if declared_family and inferred_family and declared_family != inferred_family:
            mismatches.append(
                f"`{row['column_name']}` (declared `{declared}`, inferred `{inferred}`)"
            )
    if mismatches:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has columns whose declared type does not match the type its SQL produces: {', '.join(mismatches)}."
        )


@check(code="IS005")
def check_model_columns_have_lineage(
    model, ctx, *, exclude_column_name_pattern: RegexPattern | None = None
):
    """Each column of a model with upstream dependencies must have column-level lineage.

    !!! info "Rationale"

        Column-level lineage powers impact analysis and the propagation checks in this category. A model that dbt's static analysis cannot analyse has no lineage at all, and a column declared in YAML that the SQL does not produce has no lineage either. This check reports both, so gaps in lineage are visible instead of silently weakening every check that depends on it.

    !!! note

        This check requires the dbt Information Schema (dbt 2.0+, `--generate-info-schema`). Models without upstream dependencies are not checked. Ephemeral models are not checked either: dbt inlines them, so their lineage is recorded on the models that select from them. Columns built only from literals (e.g. `'web' as channel`) have no upstream column: exclude them with `exclude_column_name_pattern`.

    Parameters:
        exclude_column_name_pattern (str | None): Regex pattern to match column names that do not need lineage.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        info_schema_checks:
            - name: check_model_columns_have_lineage
              exclude_column_name_pattern: ^_loaded_at$
        ```

    """
    if not (model.depends_on and model.depends_on.nodes):
        return
    if model.config and model.config.materialized == "ephemeral":
        return
    info_schema: "InfoSchema" = ctx.info_schema
    model_name = get_clean_model_name(model.unique_id)
    lineage = info_schema.lineage_by_child.get(model.unique_id)
    if not lineage:
        fail(
            f"`{model_name}` has no column-level lineage. Run dbt with `--static-analysis strict` and check the dbt logs for static analysis errors on this model."
        )
    exclude = (
        re.compile(exclude_column_name_pattern.strip())
        if exclude_column_name_pattern
        else None
    )
    without_lineage = [
        row["column_name"]
        for key, row in sorted(
            info_schema.node_columns.get(model.unique_id, {}).items()
        )
        if key not in lineage and not (exclude and exclude.match(row["column_name"]))
    ]
    if without_lineage:
        fail(
            f"`{model_name}` has columns without column-level lineage: {', '.join(f'`{c}`' for c in without_lineage)}."
        )


@check(code="IS008")
def check_model_public_columns_not_derived_from_meta(model, ctx, *, meta_key: str):
    """Public models must not expose columns derived from a column that sets a `meta` key.

    !!! info "Rationale"

        Public models are the interface that other teams and projects build on, so anything they expose spreads beyond the owning team. When a column classified with a `meta` key, for example `pii: true`, flows into a public model, possibly through several intermediate models, sensitive data leaves the team's control. This check walks dbt's column-level lineage upstream from every column of a public model and fails when any ancestor column sets the key.

    !!! note

        This check requires the dbt Information Schema (dbt 2.0+, `--generate-info-schema`). It follows `copy` and `mod` lineage edges through any number of models, not `scan` edges. Column `meta` is read from `manifest.json`. Models that are not public are not checked.

    Parameters:
        meta_key (str): The `meta` key that marks a sensitive column, e.g. `pii`. A column is sensitive when the key has a truthy value.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        info_schema_checks:
            - name: check_model_public_columns_not_derived_from_meta
              meta_key: pii
        ```

    """
    if model.access != "public":
        return
    info_schema: "InfoSchema" = ctx.info_schema
    exposed: list[str] = []
    for key in sorted(info_schema.node_columns.get(model.unique_id, {})):
        column_name = _column_name(info_schema, model.unique_id, key)
        # Breadth-first walk upstream; `seen` stops cycles and repeated paths.
        queue: list[tuple[str, str]] = [(model.unique_id, key)]
        seen: set[tuple[str, str]] = set()
        while queue:
            unique_id, column_key = queue.pop(0)
            if (unique_id, column_key) in seen:
                continue
            seen.add((unique_id, column_key))
            source_column = _column_name(info_schema, unique_id, column_key)
            if _has_meta_key(ctx, unique_id, source_column, meta_key):
                exposed.append(f"`{column_name}` (from `{unique_id}.{source_column}`)")
                break
            queue.extend(
                (e.parent_node_unique_id, e.parent_column_name.casefold())
                for e in _value_edges(
                    info_schema.lineage_by_child.get(unique_id, {}).get(column_key, [])
                )
            )
    if exposed:
        fail(
            f"Public model `{get_clean_model_name(model.unique_id)}` exposes columns derived from a column with `meta.{meta_key}`: {', '.join(exposed)}."
        )
