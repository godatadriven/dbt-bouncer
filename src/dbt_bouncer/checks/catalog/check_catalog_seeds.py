from typing import Any

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import get_clean_model_name

# The default stat-key fallback lists below were verified from each adapter's source:
#   - Snowflake (include/snowflake/macros/catalog.sql):           ``row_count``, ``bytes``
#   - BigQuery  (include/bigquery/macros/catalog/catalog.sql):    ``num_rows``,  ``num_bytes``
#   - Redshift  (include/redshift/macros/catalog/catalog.sql):    ``rows``,      ``size``
#   - Databricks/Spark (dbt-spark `convert_table_stats`, parsed
#     from ``DESCRIBE TABLE EXTENDED`` output of the form
#     ``"<bytes> bytes, <rows> rows"``):                          ``rows``,      ``bytes``
# For any other adapter (Athena, Fabric, Trino, ClickHouse, Synapse, DuckDB,
# Postgres, and assorted community adapters), users can supply the relevant
# key(s) via the ``row_stat_keys`` / ``byte_stat_keys`` check params.


def _extract_stat_value(catalog_node: Any, stat_keys: list[str]) -> int | None:
    """Return the first numeric stat value found on ``catalog_node`` for any of ``stat_keys``.

    Returns:
        int | None: The matched stat value coerced to ``int``, or ``None`` when stats
        are absent or none of the supplied keys are present with a numeric value
        (signalling that the active adapter does not expose this stat under any
        of the configured keys).

    """
    stats = catalog_node.stats
    if stats is None:
        return None
    # ``has_stats`` is the dbt convention for adapters that emit no stats at all (e.g. DuckDB).
    # Access via getattr so the DictProxy's lazy wrapping returns a proxy whose ``.value`` works.
    # Use truthy comparison rather than ``is False`` so that any falsy adapter-side
    # representation (``0``, empty string, etc.) is also treated as "no stats".
    has_stats_entry = getattr(stats, "has_stats", None)
    if has_stats_entry is not None and not has_stats_entry.value:
        return None
    for key in stat_keys:
        if key not in stats:
            continue
        value = stats[key].value
        if isinstance(value, (int, float)):
            return int(value)
    return None


@check(code="CA001")
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
        exclude (str | list[str] | None): Regex pattern(s) to match the seed path. Seed paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the seed path. Only seed paths that match any pattern will be checked.
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


@check(code="CA002")
def check_seed_max_bytes(
    catalog_node,
    *,
    max_bytes: int,
    byte_stat_keys: list[str] = ["bytes", "num_bytes", "size"],  # noqa: B006
):
    """Each seed must not exceed the given size in bytes.

    !!! info "Rationale"

        Seeds are checked into version control and reloaded by every dbt run, so very large CSV files inflate repository size, slow seed runs, and signal that the data probably belongs in a source table instead. This check enforces an upper bound on seed size so that the project does not accumulate oversized reference data.

    !!! note

        Seed size is read from the catalog's per-relation stats, which are populated by the warehouse adapter. The default `byte_stat_keys` cover the adapters whose source has been verified: `dbt-snowflake` (`bytes`), `dbt-bigquery` (`num_bytes`), `dbt-redshift` (`size`), and `dbt-databricks`/`dbt-spark` (`bytes`, parsed from `DESCRIBE TABLE EXTENDED`). The first key found on the catalog node wins.

        For any other adapter — e.g. `dbt-athena`, `dbt-fabric`, `dbt-trino`, `dbt-clickhouse`, `dbt-synapse`, `dbt-singlestore`, `dbt-vertica`, etc. — inspect the relevant entry in `catalog.json` to find which key (if any) holds the byte count, then supply it via `byte_stat_keys`. If the adapter emits no byte stats at all the check will raise a `RuntimeError`.

    Parameters:
        max_bytes (int): The maximum number of bytes permitted for a seed.
        byte_stat_keys (list[str]): Ordered list of stat keys under `stats.<key>.value` to try when extracting the seed's byte count. Defaults to `["bytes", "num_bytes", "size"]`, which covers the verified adapters. Override to support additional adapters.

    Receives:
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the seed path. Seed paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the seed path. Only seed paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Raises:
        RuntimeError: If the active adapter does not expose any of `byte_stat_keys` in the catalog.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_seed_max_bytes
              max_bytes: 1048576
        ```
        ```yaml
        # Overriding the keys for a hypothetical adapter that exposes ``size_bytes``.
        catalog_checks:
            - name: check_seed_max_bytes
              max_bytes: 1048576
              byte_stat_keys:
                - size_bytes
        ```

    """
    if catalog_node.unique_id is None or not catalog_node.unique_id.startswith("seed."):
        return

    if max_bytes <= 0:
        raise ValueError(f"`max_bytes` must be positive, got {max_bytes}.")
    if not byte_stat_keys:
        raise ValueError("`byte_stat_keys` must not be empty.")

    bytes_used = _extract_stat_value(catalog_node, byte_stat_keys)
    if bytes_used is None:
        raise RuntimeError(
            f"`{get_clean_model_name(catalog_node.unique_id)}` does not expose any of "
            f"{list(byte_stat_keys)} in catalog stats. If your adapter emits byte "
            "stats under a different key, set `byte_stat_keys` accordingly. If it "
            "emits no byte stats at all, this check is not supported for that adapter."
        )

    if bytes_used > max_bytes:
        fail(
            f"`{get_clean_model_name(catalog_node.unique_id)}` has {bytes_used} bytes, "
            f"which is greater than the permitted {max_bytes}."
        )


@check(code="CA003")
def check_seed_max_row_count(
    catalog_node,
    *,
    max_row_count: int,
    row_stat_keys: list[str] = ["row_count", "num_rows", "rows"],  # noqa: B006
):
    """Each seed must not contain more than the given number of rows.

    !!! info "Rationale"

        Seeds are intended for small reference datasets such as lookup tables, country codes, or feature flag values. As row counts grow, seeds become slow to load, awkward to review in pull requests, and prone to drift from authoritative upstream systems. This check enforces an upper bound on row count so that large datasets are surfaced as a source or external table instead of a seed.

    !!! note

        Row count is read from the catalog's per-relation stats, which are populated by the warehouse adapter. The default `row_stat_keys` cover the adapters whose source has been verified: `dbt-snowflake` (`row_count`), `dbt-bigquery` (`num_rows`), `dbt-redshift` (`rows`), and `dbt-databricks`/`dbt-spark` (`rows`, parsed from `DESCRIBE TABLE EXTENDED`). The first key found on the catalog node wins.

        For any other adapter — e.g. `dbt-athena`, `dbt-fabric`, `dbt-trino`, `dbt-clickhouse`, `dbt-synapse`, `dbt-singlestore`, `dbt-vertica`, etc. — inspect the relevant entry in `catalog.json` to find which key (if any) holds the row count, then supply it via `row_stat_keys`. If the adapter emits no row stats at all the check will raise a `RuntimeError`.

    Parameters:
        max_row_count (int): The maximum number of rows permitted for a seed.
        row_stat_keys (list[str]): Ordered list of stat keys under `stats.<key>.value` to try when extracting the seed's row count. Defaults to `["row_count", "num_rows", "rows"]`, which covers the verified adapters. Override to support additional adapters.

    Receives:
        catalog_node (CatalogNodeEntry): The CatalogNodeEntry object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the seed path. Seed paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the seed path. Only seed paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Raises:
        RuntimeError: If the active adapter does not expose any of `row_stat_keys` in the catalog.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_seed_max_row_count
              max_row_count: 1000
        ```
        ```yaml
        # Overriding the keys for a hypothetical adapter that exposes ``record_count``.
        catalog_checks:
            - name: check_seed_max_row_count
              max_row_count: 1000
              row_stat_keys:
                - record_count
        ```

    """
    if catalog_node.unique_id is None or not catalog_node.unique_id.startswith("seed."):
        return

    if max_row_count <= 0:
        raise ValueError(f"`max_row_count` must be positive, got {max_row_count}.")
    if not row_stat_keys:
        raise ValueError("`row_stat_keys` must not be empty.")

    row_count = _extract_stat_value(catalog_node, row_stat_keys)
    if row_count is None:
        raise RuntimeError(
            f"`{get_clean_model_name(catalog_node.unique_id)}` does not expose any of "
            f"{list(row_stat_keys)} in catalog stats. If your adapter emits row "
            "stats under a different key, set `row_stat_keys` accordingly. If it "
            "emits no row stats at all, this check is not supported for that adapter."
        )

    if row_count > max_row_count:
        fail(
            f"`{get_clean_model_name(catalog_node.unique_id)}` has {row_count} rows, "
            f"which is greater than the permitted {max_row_count}."
        )
