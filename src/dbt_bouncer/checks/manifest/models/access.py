"""Checks related to model access controls and contract enforcement."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


@check
def check_model_access(model, *, access: str):
    """Models must have the specified access attribute. Requires dbt 1.7+.

    Parameters:
        access (Literal["private", "protected", "public"]): The access level to check for.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            # Align with dbt best practices that marts should be `public`, everything else should be `protected`
            - name: check_model_access
              access: protected
              include: ^models/intermediate
            - name: check_model_access
              access: public
              include: ^models/marts
            - name: check_model_access
              access: protected
              include: ^models/staging
        ```

    """
    if model.access and model.access.value != access:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has `{model.access.value}` access, it should have access `{access}`."
        )


@check
def check_model_contract_enforced_for_public_model(model):
    """Public models must have contracts enforced.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_contract_enforced_for_public_model
        ```

    """
    if (
        model.access
        and model.access.value == "public"
        and (not model.contract or model.contract.enforced is not True)
    ):
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is a public model but does not have contracts enforced."
        )


@check
def check_model_grant_privilege(model, *, privilege_pattern: str):
    """Model can have grant privileges that match the specified pattern.

    Receives:
        model (ModelNode): The ModelNode object to check.
        privilege_pattern (str): Regex pattern to match the privilege.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_grant_privilege
              include: ^models/marts
              privilege_pattern: ^select
        ```

    """
    compiled = compile_pattern(privilege_pattern.strip())
    config = model.config
    grants = config.grants if config else {}
    non_complying_grants = [i for i in (grants or {}) if compiled.match(str(i)) is None]

    if non_complying_grants:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has grants (`{privilege_pattern}`) that don't comply with the specified regexp pattern ({non_complying_grants})."
        )


@check
def check_model_grant_privilege_required(model, *, privilege: str):
    """Model must have the specified grant privilege.

    Receives:
        model (ModelNode): The ModelNode object to check.
        privilege (str): The privilege that is required.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_grant_privilege_required
              include: ^models/marts
              privilege: select
        ```

    """
    config = model.config
    grants = config.grants if config else {}
    if privilege not in (grants or {}):
        fail(
            f"`{get_clean_model_name(model.unique_id)}` does not have the required grant privilege (`{privilege}`)."
        )


@check
def check_model_has_contracts_enforced(model):
    """Model must have contracts enforced.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_contracts_enforced
              include: ^models/marts
        ```

    """
    if not model.contract or model.contract.enforced is not True:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` does not have contracts enforced."
        )


@check
def check_model_number_of_grants(
    model, *, max_number_of_privileges: int = 100, min_number_of_privileges: int = 0
):
    """Model can have the specified number of privileges.

    Receives:
        max_number_of_privileges (int | None): Maximum number of privileges, inclusive.
        min_number_of_privileges (int | None): Minimum number of privileges, inclusive.
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_number_of_grants
              include: ^models/marts
              max_number_of_privileges: 1 # Optional
              min_number_of_privileges: 0 # Optional
        ```

    """
    if min_number_of_privileges < 0:
        raise ValueError(
            f"`min_number_of_privileges` must be non-negative, got {min_number_of_privileges}."
        )
    if max_number_of_privileges <= 0:
        raise ValueError(
            f"`max_number_of_privileges` must be positive, got {max_number_of_privileges}."
        )
    if min_number_of_privileges > max_number_of_privileges:
        raise ValueError(
            f"`min_number_of_privileges` ({min_number_of_privileges}) must not exceed `max_number_of_privileges` ({max_number_of_privileges})."
        )

    config = model.config
    grants = config.grants if config else {}
    num_grants = len((grants or {}).keys())

    if num_grants < min_number_of_privileges:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has less grants (`{num_grants}`) than the specified minimum ({min_number_of_privileges})."
        )
    if num_grants > max_number_of_privileges:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has more grants (`{num_grants}`) than the specified maximum ({max_number_of_privileges})."
        )
