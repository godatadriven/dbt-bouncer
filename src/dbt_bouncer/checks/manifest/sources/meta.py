"""Checks related to source meta configuration."""

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.check_framework.exceptions import NestedDict
from dbt_bouncer.utils import compile_pattern, find_missing_meta_keys


@check
def check_source_has_labels_keys(source, *, keys: NestedDict):
    """The `labels` config for sources must have the specified keys.

    !!! info "Rationale"

        Labels are key-value pairs attached to warehouse resources (e.g. BigQuery table labels) that drive cost attribution, governance workflows, and access control. Requiring specific label keys on sources ensures that the same ownership and environment metadata required on models is also enforced at the ingestion boundary, giving a consistent labelling policy across the full data platform.

    !!! note

        dbt 2.0 (Fusion) does not permit `labels` as a top-level config on a source
        table, so `labels` must be nested under `meta` (e.g. `config.meta.labels`).
        This check looks in both locations — the legacy top-level `config.labels`
        and `config.meta.labels` — so it works before and after a Fusion migration
        (e.g. one applied by `dbt-autofix`).

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

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
            - name: check_source_has_labels_keys
              keys:
                - env
                - team
        ```

    """
    # `labels` may live at the top level (`config.labels`, valid for models and
    # source-level configs) or nested under `meta` (`config.meta.labels`, the only
    # location dbt 2.0/Fusion allows on a source table). Union both so a required
    # key present in either location satisfies the check; the Fusion location wins
    # on any key clash.
    top_level_labels = getattr(source.config, "labels", None) or {}
    meta = getattr(source.config, "meta", None) or {}
    meta_labels = (meta.get("labels") if isinstance(meta, dict) else None) or {}
    labels = {**top_level_labels, **meta_labels}
    missing_keys = find_missing_meta_keys(
        meta_config=labels, required_keys=keys.model_dump()
    )
    if missing_keys:
        display = f"{source.source_name}.{source.name}"
        fail(
            f"`{display}` is missing the following keys from the `labels` config: {[x.replace('>>', '') for x in missing_keys]}"
        )


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
        exclude (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Source paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Only source paths that match any pattern will be checked.
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


@check
def check_source_pii_meta(source, *, column_name_pattern: str, meta_key: str):
    """Source columns matching a PII pattern must carry a governance meta key.

    !!! info "Rationale"

        PII columns need explicit classification metadata for access control and compliance reporting. Without enforcing a required meta key on columns whose names indicate sensitive data, teams silently omit privacy labels, making it impossible to automate data masking, access control policies, or GDPR/CCPA audit trails.

    Parameters:
        column_name_pattern (str): Regex pattern to match column names that are considered PII.
        meta_key (str): The meta key that must be present and non-empty on each matching column.
            The key must be present with a truthy value; a falsy value (e.g. ``false``, ``0``,
            empty string) is treated as missing.

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
            - name: check_source_pii_meta
              column_name_pattern: ^(email|phone|address|ssn).*
              meta_key: pii
        ```

    """
    compiled = compile_pattern(column_name_pattern.strip())
    offending = [
        name
        for name, col in (source.columns or {}).items()
        if compiled.match(str(name))
        and not (getattr(col, "meta", None) or {}).get(meta_key)
    ]
    if offending:
        fail(
            f"Source `{source.source_name}.{source.name}` has PII-pattern columns missing `meta.{meta_key}`: {offending}."
        )
