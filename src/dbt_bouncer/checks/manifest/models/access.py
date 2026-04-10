"""Checks related to model access controls and contract enforcement."""

from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


@check
def check_model_access(model, *, access: str):
    """Models must have the specified access attribute. Requires dbt 1.7+.

    !!! info "Rationale"

        Access controls determine which models can be referenced across dbt projects and packages. Enforcing access levels ensures that staging models remain internal (`protected`), while only curated mart models are exposed as `public` — preventing downstream consumers from depending on unstable intermediate transformations.

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

    !!! info "Rationale"

        Public models form the API surface of your dbt project. Without enforced contracts, column additions, removals, or type changes can silently break downstream consumers. This check ensures that every public model guarantees its schema, catching breaking changes at build time rather than in production.

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

    !!! info "Rationale"

        Uncontrolled grant names can lead to excessive or incorrectly named privileges being applied to models, making it difficult to audit who has access to what. Enforcing a naming pattern for grants ensures consistency and makes security reviews straightforward.

    Parameters:
        privilege_pattern (str): Regex pattern to match the privilege.

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

    !!! info "Rationale"

        Mart models often need to be readable by BI tools or downstream consumers. Requiring a specific grant privilege (e.g. `select`) ensures that access is explicitly configured and not left to database defaults, which may vary across environments.

    Parameters:
        privilege (str): The privilege that is required.

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

    !!! info "Rationale"

        Enforced contracts guarantee that a model's output schema — column names and types — is validated at build time. Without this, schema changes can silently break downstream consumers. Applying this check to a specific set of models (e.g. marts) provides a schema stability guarantee for those critical outputs.

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
    model,
    *,
    max_number_of_privileges: Annotated[int, Field(gt=0)] = 100,
    min_number_of_privileges: Annotated[int, Field(ge=0)] = 0,
):
    """Model can have the specified number of privileges.

    !!! info "Rationale"

        An unexpectedly large number of grants on a model may indicate privilege creep, where access has accumulated over time without a systematic review. Bounding the number of grants encourages a deliberate access-control strategy and makes security audits easier.

    Parameters:
        max_number_of_privileges (int | None): Maximum number of privileges, inclusive.
        min_number_of_privileges (int | None): Minimum number of privileges, inclusive.

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
            - name: check_model_number_of_grants
              include: ^models/marts
              max_number_of_privileges: 1 # Optional
              min_number_of_privileges: 0 # Optional
        ```

    """
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
