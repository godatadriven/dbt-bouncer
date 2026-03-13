from dbt_bouncer.check_decorator import check, fail


@check
def check_test_has_tags(test, *, criteria: str = "all", tags: list[str]):
    """Data tests must have the specified tags.

    Parameters:
        criteria (Literal["any", "all", "one"] | None): Whether the test must have any, all, or exactly one of the specified tags. Default: `any`.
        tags (list[str]): List of tags to check for.

    Receives:
        test (TestNode): The TestNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the test path. Test paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the test path. Only test paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_test_has_tags
              tags:
                - critical
        ```

    """
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
