from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import (
    compile_pattern,
    get_clean_model_name,
    is_description_populated,
)


@check
def check_snapshot_description_populated(
    snapshot, *, min_description_length: Annotated[int, Field(gt=0)] | None = None
):
    """Snapshots must have a populated description.

    !!! info "Rationale"

        Snapshots capture slowly-changing dimension (SCD) history and represent some of the most business-critical data in a dbt project. Without descriptions, it is unclear what entity a snapshot tracks, what the key fields represent, or how frequently it is refreshed — information that is essential for analysts interpreting historical data and for engineers maintaining the snapshot strategy.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

    Receives:
        snapshot (SnapshotNode): The SnapshotNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the snapshot path. Snapshot paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the snapshot path. Only snapshot paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_snapshot_description_populated
        ```
        ```yaml
        manifest_checks:
            - name: check_snapshot_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """
    if not is_description_populated(
        snapshot.description or "", min_description_length or 4
    ):
        fail(
            f"`{get_clean_model_name(snapshot.unique_id)}` does not have a populated description."
        )


@check
def check_snapshot_has_tags(snapshot, *, criteria: str = "all", tags: list[str]):
    """Snapshots must have the specified tags.

    !!! info "Rationale"

        Tags on snapshots enable selective execution (e.g. `dbt snapshot --select tag:nightly`) and make it possible to apply governance policies to specific groups of snapshots. Without enforced tagging, snapshots can be inadvertently skipped in scheduled runs or included in the wrong execution contexts, leading to stale historical data.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the snapshot must have any, all, or exactly one of the specified tags. Default: `all`.
        tags (list[str]): List of tags to check for.

    Receives:
        snapshot (SnapshotNode): The SnapshotNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the snapshot path. Snapshot paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the snapshot path. Only snapshot paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_snapshot_has_tags
              tags:
                - tag_1
                - tag_2
        ```

    """
    resource_tags = snapshot.tags or []
    if criteria == "any":
        if not any(tag in resource_tags for tag in tags):
            fail(f"`{snapshot.name}` does not have any of the required tags: {tags}.")
    elif criteria == "all":
        missing_tags = [tag for tag in tags if tag not in resource_tags]
        if missing_tags:
            fail(f"`{snapshot.name}` is missing required tags: {missing_tags}.")
    elif criteria == "one" and sum(tag in resource_tags for tag in tags) != 1:
        fail(f"`{snapshot.name}` must have exactly one of the required tags: {tags}.")


@check
def check_snapshot_names(snapshot, *, snapshot_name_pattern: str):
    """Snapshots must have a name that matches the supplied regex.

    !!! info "Rationale"

        A consistent naming convention for snapshots (e.g. a domain prefix like `erp_` or a suffix like `_snapshot`) makes it immediately obvious in the warehouse that a table is a point-in-time history capture rather than a regular dimension or fact. Without naming conventions, snapshots can be confused with other tables, leading to incorrect use or accidental truncation.

    Parameters:
        snapshot_name_pattern (str): Regexp the snapshot name must match.

    Receives:
        snapshot (SnapshotNode): The SnapshotNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the snapshot path. Snapshot paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the snapshot path. Only snapshot paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_snapshot_names
              include: ^snapshots/erp
              snapshot_name_pattern: ^erp
        ```

    """
    compiled_pattern = compile_pattern(snapshot_name_pattern.strip())
    if compiled_pattern.match(str(snapshot.name)) is None:
        fail(
            f"`{snapshot.name}` does not match the supplied regex `{snapshot_name_pattern.strip()}`."
        )
