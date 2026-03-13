from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import compile_pattern


@check("check_snapshot_has_tags", iterate_over="snapshot")
def check_snapshot_has_tags(snapshot, *, criteria: str = "all", tags: list[str]):
    """Snapshots must have the specified tags."""
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


@check("check_snapshot_names", iterate_over="snapshot")
def check_snapshot_names(snapshot, *, snapshot_name_pattern: str):
    """Snapshots must have a name that matches the supplied regex."""
    compiled_pattern = compile_pattern(snapshot_name_pattern.strip())
    if compiled_pattern.match(str(snapshot.name)) is None:
        fail(
            f"`{snapshot.name}` does not match the supplied regex `{snapshot_name_pattern.strip()}`."
        )
