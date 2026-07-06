from typing import Annotated, Literal

from pydantic import Field

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.check_framework.exceptions import NestedDict
from dbt_bouncer.utils import (
    compile_pattern,
    find_missing_meta_keys,
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
        exclude (str | list[str] | None): Regex pattern(s) to match the snapshot path. Snapshot paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the snapshot path. Only snapshot paths that match any pattern will be checked.
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
def check_snapshot_has_meta_keys(snapshot, *, keys: NestedDict):
    """The `meta` config for snapshots must have the specified keys.

    !!! info "Rationale"

        The `meta` config is a flexible, project-defined dictionary used to track ownership, maturity levels, PII classification, and other governance attributes. Requiring specific keys ensures that these attributes are consistently populated across all snapshots, enabling automated reporting, data cataloguing, and access-control workflows that depend on them.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

    Receives:
        snapshot (SnapshotNode): The SnapshotNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the snapshot path. Snapshot paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the snapshot path. Only snapshot paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_snapshot_has_meta_keys
              keys:
                - maturity
                - owner
        ```

    """
    missing_keys = find_missing_meta_keys(
        meta_config=snapshot.meta or {}, required_keys=keys.model_dump()
    )
    if missing_keys:
        fail(
            f"`{snapshot.name}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
        )


@check
def check_snapshot_has_tags(
    snapshot, *, criteria: Literal["any", "all", "one"] = "all", tags: list[str]
):
    """Snapshots must have the specified tags.

    !!! info "Rationale"

        Tags on snapshots enable selective execution (e.g. `dbt snapshot --select tag:nightly`) and make it possible to apply governance policies to specific groups of snapshots. Without enforced tagging, snapshots can be inadvertently skipped in scheduled runs or included in the wrong execution contexts, leading to stale historical data.

    Parameters:
        criteria (Literal["any", "all", "one"] | None): Whether the snapshot must have any, all, or exactly one of the specified tags. Default: `all`.
        tags (list[str]): List of tags to check for.

    Receives:
        snapshot (SnapshotNode): The SnapshotNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the snapshot path. Snapshot paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the snapshot path. Only snapshot paths that match any pattern will be checked.
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
def check_snapshot_has_unique_key(snapshot):
    """Snapshots must have a `unique_key` configured.

    !!! info "Rationale"

        The `unique_key` configuration is essential for snapshot correctness — dbt uses it to identify existing rows when deciding whether to insert a new record or close an old one. Without a `unique_key`, dbt cannot reliably track which rows have changed, leading to duplicate history records or missed updates that silently corrupt the slowly-changing dimension table.

    Receives:
        snapshot (SnapshotNode): The SnapshotNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the snapshot path. Snapshot paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the snapshot path. Only snapshot paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_snapshot_has_unique_key
        ```

    """
    config = snapshot.config or {}
    if not config.get("unique_key"):
        fail(f"`{snapshot.name}` does not have a `unique_key` configured.")


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
        exclude (str | list[str] | None): Regex pattern(s) to match the snapshot path. Snapshot paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the snapshot path. Only snapshot paths that match any pattern will be checked.
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


@check
def check_snapshot_strategy(
    snapshot,
    *,
    allowed_strategies: list[str] = ["check", "timestamp"],  # noqa: B006
):
    """Snapshots must use an allowed strategy and have the required strategy-specific configuration.

    !!! info "Rationale"

        The snapshot strategy determines how dbt detects row changes. Using an unsupported or misconfigured strategy can lead to incorrect history capture: the `timestamp` strategy requires an `updated_at` column to detect changes, and the `check` strategy requires `check_cols` to define which columns to monitor. Enforcing both the allowed strategies and their required fields prevents silent data quality issues in slowly-changing dimension tables.

    Parameters:
        allowed_strategies (list[str]): List of permitted snapshot strategies. Default: `["check", "timestamp"]`.

    Receives:
        snapshot (SnapshotNode): The SnapshotNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the snapshot path. Snapshot paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the snapshot path. Only snapshot paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_snapshot_strategy
        ```
        ```yaml
        manifest_checks:
            - name: check_snapshot_strategy
              allowed_strategies:
                - timestamp # Only allow the timestamp strategy
        ```

    """
    config = snapshot.config or {}
    strategy = config.get("strategy")
    if strategy not in allowed_strategies:
        fail(
            f"`{snapshot.name}` has strategy `{strategy}`, which is not one of the allowed strategies: {allowed_strategies}."
        )
    if strategy == "timestamp" and not config.get("updated_at"):
        fail(
            f"`{snapshot.name}` uses the `timestamp` strategy but has no `updated_at` configured."
        )
    if strategy == "check" and not config.get("check_cols"):
        fail(
            f"`{snapshot.name}` uses the `check` strategy but has no `check_cols` configured."
        )
