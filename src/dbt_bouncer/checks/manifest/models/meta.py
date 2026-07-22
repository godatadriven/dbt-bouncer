"""Checks related to model meta configuration."""

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.check_framework.exceptions import NestedDict
from dbt_bouncer.utils import find_missing_meta_keys, get_clean_model_name


@check(code="MO036")
def check_model_has_labels_keys(model, *, keys: NestedDict):
    """The `labels` config for models must have the specified keys.

    !!! info "Rationale"

        Labels are key-value pairs attached to warehouse resources (e.g. BigQuery table labels) that drive cost attribution, governance workflows, and access control. Requiring specific label keys ensures that ownership and environment metadata are consistently populated across all models, enabling automated billing reports and policy enforcement that depend on those labels being present.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

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
        manifest_checks:
            - name: check_model_has_labels_keys
              keys:
                - env
                - team
        ```

    """
    labels = getattr(model.config, "labels", None) or {}
    missing_keys = find_missing_meta_keys(
        meta_config=labels, required_keys=keys.model_dump()
    )
    if missing_keys:
        display_name = get_clean_model_name(model.unique_id)
        fail(
            f"`{display_name}` is missing the following keys from the `labels` config: {[x.replace('>>', '') for x in missing_keys]}"
        )


@check(code="MO037")
def check_model_has_meta_keys(model, *, keys: NestedDict):
    """The `meta` config for models must have the specified keys.

    !!! info "Rationale"

        The `meta` config is a flexible, project-defined dictionary used to track ownership, maturity levels, PII classification, and other governance attributes. Requiring specific keys ensures that these attributes are consistently populated across all models, enabling automated reporting, data cataloguing, and access-control workflows that depend on them.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

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
        manifest_checks:
            - name: check_model_has_meta_keys
              keys:
                - maturity
                - owner
        ```

    """
    missing_keys = find_missing_meta_keys(
        meta_config=model.meta, required_keys=keys.model_dump()
    )
    if missing_keys:
        display_name = get_clean_model_name(model.unique_id)
        fail(
            f"`{display_name}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
        )
