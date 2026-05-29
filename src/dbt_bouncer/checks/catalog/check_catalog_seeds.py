from typing import Any

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import get_clean_model_name

# Adapter-specific catalog stat keys. The first match wins per resource.
# Verified from each adapter's source:
#   - Snowflake (include/snowflake/macros/catalog.sql):            ``row_count``, ``bytes``
#   - BigQuery  (include/bigquery/macros/catalog/catalog.sql):     ``num_rows``,  ``num_bytes``
#   - Redshift  (include/redshift/macros/catalog/catalog.sql):     ``rows``,      ``size``
#   - Databricks/Spark (dbt-spark `DatabricksColumn.convert_table_stats`,
#     parsed from ``DESCRIBE TABLE EXTENDED`` output of the form
#     ``"<bytes> bytes, <rows> rows"``):                           ``rows``,      ``bytes``
# dbt-postgres and dbt-duckdb emit no row/byte stats, so this check raises
# ``RuntimeError`` on those adapters.
_ROW_COUNT_STAT_KEYS = ("row_count", "num_rows", "rows")
_BYTE_COUNT_STAT_KEYS = ("bytes", "num_bytes", "size")


def _extract_stat_value(catalog_node: Any, stat_keys: tuple[str, ...]) -> int | None:
    """Return the first numeric stat value found on ``catalog_node`` for any of ``stat_keys``.

    Returns:
        int | None: The matched stat value coerced to ``int``, or ``None`` when stats
        are absent or none of the supplied keys are present with a numeric value
        (signalling that the active adapter does not expose this stat).

    """
    stats = catalog_node.stats
    if stats is None:
        return None
    # ``has_stats`` is the dbt convention for adapters that emit no stats at all (e.g. DuckDB).
    # Access via getattr so the DictProxy's lazy wrapping returns a proxy whose ``.value`` works.
    has_stats_entry = getattr(stats, "has_stats", None)
    if has_stats_entry is not None and has_stats_entry.value is False:
        return None
    for key in stat_keys:
        if key not in stats:
            continue
        value = stats[key].value
        if isinstance(value, (int, float)):
            return int(value)
    return None


@check
def check_seed_columns_are_all_documented(catalog_node, ctx):
    """All columns in a seed CSV file should be included in the seed's properties file, i.e. `.yml` file.

    !!! warning

        This check is only supported for dbt 1.9.0 and above.

    !!! info "Rationale"

        Seed CSV files often serve as reference data (e.g. country codes, product categories) that are queried directly by downstream models. When a column exists in the CSV but not in the properties file, it is invisible to documentation tools, data catalogues, and column-level tests. This check ensures that every column in a seed is explicitly declared, making it easier for consumers to understand the seed's schema and for teams to apply descriptions and tests uniformly.

    Receives:
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.
        manifest_obj (ManifestObject): The ManifestObject object parsed from `manifest.json`.
        seeds (list[SeedNode]): List of SeedNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_seed_columns_are_all_documented
        ```

    """
    if catalog_node.unique_id is not None and catalog_node.unique_id.startswith(
        "seed."
    ):
        seed = next(s for s in ctx.seeds if s.unique_id == catalog_node.unique_id)

        seed_columns = seed.columns or {}
        undocumented_columns = [
            v.name
            for _, v in catalog_node.columns.items()
            if v.name not in seed_columns
        ]

        if undocumented_columns:
            fail(
                f"`{get_clean_model_name(seed.unique_id)}` has columns that are not included in the seed properties file: {undocumented_columns}"
            )


@check
def check_seed_max_bytes(catalog_node, *, max_bytes: int):
    """Each seed must not exceed the given size in bytes.

    !!! info "Rationale"

        Seeds are checked into version control and reloaded by every dbt run, so very large CSV files inflate repository size, slow seed runs, and signal that the data probably belongs in a source table instead. This check enforces an upper bound on seed size so that the project does not accumulate oversized reference data.

    !!! note

        Seed size is read from the catalog's per-relation stats, which are populated by the warehouse adapter. Only the following adapters are known to emit byte stats: `dbt-snowflake` (`bytes`), `dbt-bigquery` (`num_bytes`), `dbt-redshift` (`size`), `dbt-databricks`/`dbt-spark` (`bytes`, parsed from `DESCRIBE TABLE EXTENDED`). Adapters that do not emit byte stats (for example `dbt-duckdb`, `dbt-postgres`) will cause this check to raise a `RuntimeError`.

    Parameters:
        max_bytes (int): The maximum number of bytes permitted for a seed.

    Receives:
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Raises:
        RuntimeError: If the active adapter does not expose a byte-count stat in the catalog.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_seed_max_bytes
              max_bytes: 1048576
        ```

    """
    if catalog_node.unique_id is None or not catalog_node.unique_id.startswith("seed."):
        return

    if max_bytes <= 0:
        raise ValueError(f"`max_bytes` must be positive, got {max_bytes}.")

    bytes_used = _extract_stat_value(catalog_node, _BYTE_COUNT_STAT_KEYS)
    if bytes_used is None:
        raise RuntimeError(
            f"`{get_clean_model_name(catalog_node.unique_id)}` does not expose any of "
            f"{list(_BYTE_COUNT_STAT_KEYS)} in catalog stats. The active adapter does "
            "not appear to emit byte-count stats; this check is only supported for "
            "adapters such as `dbt-snowflake`, `dbt-bigquery`, `dbt-redshift`, "
            "and `dbt-databricks`/`dbt-spark`."
        )

    if bytes_used > max_bytes:
        fail(
            f"`{get_clean_model_name(catalog_node.unique_id)}` has {bytes_used} bytes, "
            f"which is greater than the permitted {max_bytes}."
        )


@check
def check_seed_max_row_count(catalog_node, *, max_row_count: int):
    """Each seed must not contain more than the given number of rows.

    !!! info "Rationale"

        Seeds are intended for small reference datasets such as lookup tables, country codes, or feature flag values. As row counts grow, seeds become slow to load, awkward to review in pull requests, and prone to drift from authoritative upstream systems. This check enforces an upper bound on row count so that large datasets are surfaced as a source or external table instead of a seed.

    !!! note

        Row count is read from the catalog's per-relation stats, which are populated by the warehouse adapter. Only the following adapters are known to emit row count stats: `dbt-snowflake` (`row_count`), `dbt-bigquery` (`num_rows`), `dbt-redshift` (`rows`), `dbt-databricks`/`dbt-spark` (`rows`, parsed from `DESCRIBE TABLE EXTENDED`). Adapters that do not emit row count stats (for example `dbt-duckdb`, `dbt-postgres`) will cause this check to raise a `RuntimeError`.

    Parameters:
        max_row_count (int): The maximum number of rows permitted for a seed.

    Receives:
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Raises:
        RuntimeError: If the active adapter does not expose a row-count stat in the catalog.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_seed_max_row_count
              max_row_count: 1000
        ```

    """
    if catalog_node.unique_id is None or not catalog_node.unique_id.startswith("seed."):
        return

    if max_row_count <= 0:
        raise ValueError(f"`max_row_count` must be positive, got {max_row_count}.")

    row_count = _extract_stat_value(catalog_node, _ROW_COUNT_STAT_KEYS)
    if row_count is None:
        raise RuntimeError(
            f"`{get_clean_model_name(catalog_node.unique_id)}` does not expose any of "
            f"{list(_ROW_COUNT_STAT_KEYS)} in catalog stats. The active adapter does "
            "not appear to emit row count stats; this check is only supported for "
            "adapters such as `dbt-snowflake`, `dbt-bigquery`, `dbt-redshift`, "
            "and `dbt-databricks`/`dbt-spark`."
        )

    if row_count > max_row_count:
        fail(
            f"`{get_clean_model_name(catalog_node.unique_id)}` has {row_count} rows, "
            f"which is greater than the permitted {max_row_count}."
        )
