"""Checks related to source descriptions."""

from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import is_description_populated


@check(code="SO001")
def check_source_description_populated(
    source, *, min_description_length: Annotated[int, Field(gt=0)] | None = None
):
    """Sources must have a populated description.

    !!! info "Rationale"

        Sources represent the boundary between raw, external data and the curated dbt project. A populated description explains what the source is, where it comes from, and how it is loaded — context that is invaluable when debugging data issues or onboarding new team members. Without enforcing descriptions, sources accumulate as anonymous inputs that future maintainers cannot evaluate or trust, increasing the risk of misuse or redundant ingestion pipelines.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

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
            - name: check_source_description_populated
        ```
        ```yaml
        manifest_checks:
            - name: check_source_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """
    desc = source.description or ""
    display = f"{source.source_name}.{source.name}"
    if not is_description_populated(desc, min_description_length or 4):
        fail(f"`{display}` does not have a populated description.")


@check(code="SO017")
def check_source_top_level_description_populated(
    source, *, min_description_length: Annotated[int, Field(gt=0)] | None = None
):
    """Sources must have a populated description on the top-level `sources:` entry.

    !!! info "Rationale"

        A dbt `sources:` entry describes a whole upstream system — the CRM, the payments provider, the event stream — while each entry under `tables:` describes a single relation within it. Documenting the system itself answers questions the table descriptions cannot: who owns the feed, how it lands, and what it is authoritative for. Without it, readers of the catalogue see a list of tables with no explanation of where they came from.

        This check reads the `source_description` field in `manifest.json`, i.e. the `description` of the top-level `sources:` entry. To check the description of an individual source table, use `check_source_description_populated`.

    Parameters:
        min_description_length (int | None): The minimum length of the description. Must be greater than 0.

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
            - name: check_source_top_level_description_populated
        ```
        ```yaml
        manifest_checks:
            - name: check_source_top_level_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """
    desc = source.source_description or ""
    if not is_description_populated(desc, min_description_length or 4):
        fail(
            f"Source `{source.source_name}` does not have a populated description on its top-level `sources:` entry."
        )
