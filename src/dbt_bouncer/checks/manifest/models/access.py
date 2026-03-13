"""Checks related to model access controls and contract enforcement."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


@check(
    "check_model_access",
    iterate_over="model",
    params={"access": str},
)
def check_model_access(model, ctx, *, access: str):
    """Models must have the specified access attribute. Requires dbt 1.7+."""
    if model.access and model.access.value != access:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has `{model.access.value}` access, it should have access `{access}`."
        )


@check("check_model_contract_enforced_for_public_model", iterate_over="model")
def check_model_contract_enforced_for_public_model(model, ctx):
    """Public models must have contracts enforced."""
    if (
        model.access
        and model.access.value == "public"
        and (not model.contract or model.contract.enforced is not True)
    ):
        fail(
            f"`{get_clean_model_name(model.unique_id)}` is a public model but does not have contracts enforced."
        )


@check(
    "check_model_grant_privilege",
    iterate_over="model",
    params={"privilege_pattern": str},
)
def check_model_grant_privilege(model, ctx, *, privilege_pattern: str):
    """Model can have grant privileges that match the specified pattern."""
    compiled = compile_pattern(privilege_pattern.strip())
    config = model.config
    grants = config.grants if config else {}
    non_complying_grants = [i for i in (grants or {}) if compiled.match(str(i)) is None]

    if non_complying_grants:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has grants (`{privilege_pattern}`) that don't comply with the specified regexp pattern ({non_complying_grants})."
        )


@check(
    "check_model_grant_privilege_required",
    iterate_over="model",
    params={"privilege": str},
)
def check_model_grant_privilege_required(model, ctx, *, privilege: str):
    """Model must have the specified grant privilege."""
    config = model.config
    grants = config.grants if config else {}
    if privilege not in (grants or {}):
        fail(
            f"`{get_clean_model_name(model.unique_id)}` does not have the required grant privilege (`{privilege}`)."
        )


@check("check_model_has_contracts_enforced", iterate_over="model")
def check_model_has_contracts_enforced(model, ctx):
    """Model must have contracts enforced."""
    if not model.contract or model.contract.enforced is not True:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` does not have contracts enforced."
        )


@check(
    "check_model_number_of_grants",
    iterate_over="model",
    params={
        "max_number_of_privileges": (int, 100),
        "min_number_of_privileges": (int, 0),
    },
)
def check_model_number_of_grants(
    model, ctx, *, max_number_of_privileges: int, min_number_of_privileges: int
):
    """Model can have the specified number of privileges."""
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
