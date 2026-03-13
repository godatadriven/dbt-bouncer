from dbt_bouncer.check_decorator import check, fail


@check("check_test_has_tags", iterate_over="test")
def check_test_has_tags(test, *, criteria: str = "all", tags: list[str]):
    """Data tests must have the specified tags."""
    resource_tags = test.tags or []
    if criteria == "any":
        if not any(tag in resource_tags for tag in tags):
            fail(f"`{test.unique_id}` does not have any of the required tags: {tags}.")
    elif criteria == "all":
        missing_tags = [tag for tag in tags if tag not in resource_tags]
        if missing_tags:
            fail(f"`{test.unique_id}` is missing required tags: {missing_tags}.")
    elif criteria == "one" and sum(tag in resource_tags for tag in tags) != 1:
        fail(f"`{test.unique_id}` must have exactly one of the required tags: {tags}.")
