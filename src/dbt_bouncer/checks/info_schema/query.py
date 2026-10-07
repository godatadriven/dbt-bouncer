"""A check that runs custom SQL against the dbt Information Schema."""

from dbt_bouncer.check_framework.decorator import check, fail

# Failure messages list at most this many offending rows.
_MAX_ROWS_IN_MESSAGE = 10


def _require_single_select(*, sql: str) -> None:
    """Reject SQL that is not exactly one SELECT statement.

    The Information Schema database also has external access disabled; this
    stops statements such as `COPY` or `ATTACH` before the run starts.

    Raises:
        ValueError: If `sql` does not parse, or is not a single SELECT.

    """
    import duckdb

    try:
        statements = duckdb.extract_statements(sql)
    except duckdb.Error as e:
        raise ValueError(f"`sql` is not valid SQL: {e}") from e
    if len(statements) != 1 or statements[0].type != duckdb.StatementType.SELECT:
        raise ValueError("`sql` must be exactly one SELECT statement.")


@check(code="IS001", validate=_require_single_select)
def check_info_schema_query(ctx, *, sql: str):
    """A SQL query against the dbt Information Schema must return no rows.

    !!! info "Rationale"

        Some project rules are easiest to express as a query, for example "no model joins more than three sources" or "every metric has a label". dbt 2.0 stores project metadata as tables in the dbt Information Schema, so a rule becomes a SELECT that returns the offending rows. This check runs such a query and fails when it returns any rows, with the same severity, selection and reporting as every other dbt-bouncer check. Queries written for dbt's own `dbt check` command can be reused by replacing `{{ info_schema('<table>') }}` with the table name, e.g. `dbt.models`.

    !!! note

        This check requires the dbt Information Schema (dbt 2.0+, `--generate-info-schema`). Tables are named `<schema>.<table>` as in the Parquet file names, e.g. `dbt.models`, `dbt.column_lineage`, `dbt_rt.run_results`. See the [dbt Information Schema reference](https://docs.getdbt.com/reference/info-schema) for the available tables. The query runs in an in-memory DuckDB database with file access disabled.

    Parameters:
        sql (str): A single DuckDB SELECT statement. Each row returned is a failure.

    Receives:
        info_schema (InfoSchema): The dbt Information Schema tables.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        info_schema_checks:
            - name: check_info_schema_query
              description: Public models must have a description.
              sql: |
                  SELECT unique_id
                  FROM dbt.models
                  WHERE access = 'public'
                    AND coalesce(description, '') = ''
        ```

    """
    columns, rows = ctx.info_schema.query(sql)
    if rows:
        shown = "; ".join(
            ", ".join(f"{c}={v}" for c, v in zip(columns, row, strict=True))
            for row in rows[:_MAX_ROWS_IN_MESSAGE]
        )
        more = (
            f" (first {_MAX_ROWS_IN_MESSAGE} shown)"
            if len(rows) > _MAX_ROWS_IN_MESSAGE
            else ""
        )
        fail(f"The query returned {len(rows)} row(s){more}: {shown}.")
