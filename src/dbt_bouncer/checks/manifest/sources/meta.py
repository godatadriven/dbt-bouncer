"""Checks related to source meta configuration."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.checks.common import NestedDict
from dbt_bouncer.utils import find_missing_meta_keys


@check
def check_source_has_meta_keys(source, *, keys: NestedDict):
    """The `meta` config for sources must have the specified keys.

    !!! info "Rationale"

        The `meta` config is a free-form dictionary that teams use to attach governance information to dbt nodes — things like data owner, sensitivity classification, SLA tier, or compliance labels. Without enforcing required keys, this metadata is applied inconsistently: some sources have an owner, others do not; some are labelled PII-sensitive, others are silently omitted. This check ensures that every source carries the minimum set of metadata keys needed to support data governance, access control automation, and operational runbooks.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

    Receives:
        source (SourceNode): The SourceNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Source paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the source path (i.e the .yml file where the source is configured). Only source paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_has_meta_keys
              keys:
                - contact:
                    - email
                    - slack
                - owner
        ```

    """
    display = f"{source.source_name}.{source.name}"
    missing_keys = find_missing_meta_keys(
        meta_config=source.meta, required_keys=keys.model_dump()
    )
    if missing_keys:
        fail(
            f"`{display}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
        )
