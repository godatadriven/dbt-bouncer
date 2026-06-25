"""Checks related to source loaders."""

from dbt_bouncer.check_framework.decorator import check, fail


@check
def check_source_loader_populated(source):
    """Sources must have a populated loader.

    !!! info "Rationale"

        The `loader` field documents which tool or pipeline is responsible for bringing data into the warehouse (e.g. Fivetran, Airbyte, a custom ETL script). Without it, there is no traceable link between a dbt source and the upstream process that feeds it, making it difficult to investigate data quality issues, coordinate with data engineering teams, or assess the impact of changes to the ingestion layer. Requiring a populated loader turns each source into a self-documenting artifact that points clearly to its origin.

    Receives:
        source (SourceNode): The SourceNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Source paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Only source paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_loader_populated
        ```

    """
    if source.loader == "":
        fail(f"`{source.source_name}.{source.name}` does not have a populated loader.")
