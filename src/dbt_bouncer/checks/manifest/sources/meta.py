"""Checks related to source meta configuration."""

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.checks.common import NestedDict
from dbt_bouncer.utils import find_missing_meta_keys


@check(
    "check_source_has_meta_keys",
    iterate_over="source",
    params={"keys": NestedDict},
)
def check_source_has_meta_keys(source, ctx, *, keys: NestedDict):
    """The `meta` config for sources must have the specified keys."""
    display = f"{source.source_name}.{source.name}"
    missing_keys = find_missing_meta_keys(
        meta_config=source.meta,
        required_keys=keys.model_dump(),
    )
    if missing_keys:
        fail(
            f"`{display}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
        )
