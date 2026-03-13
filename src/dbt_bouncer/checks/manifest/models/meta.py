"""Checks related to model meta configuration."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.checks.common import NestedDict
from dbt_bouncer.utils import find_missing_meta_keys, get_clean_model_name


@check(
    "check_model_has_meta_keys",
    iterate_over="model",
    params={"keys": NestedDict},
)
def check_model_has_meta_keys(model, ctx, *, keys: NestedDict):
    """The ``meta`` config for models must have the specified keys."""
    missing_keys = find_missing_meta_keys(
        meta_config=model.meta,
        required_keys=keys.model_dump(),
    )
    if missing_keys:
        display_name = get_clean_model_name(model.unique_id)
        fail(
            f"`{display_name}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
        )
