from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.check_framework.exceptions import NestedDict
from dbt_bouncer.enums import Criteria
from dbt_bouncer.utils import compile_pattern, find_missing_meta_keys


@check
def check_test_has_meta_keys(test, *, keys: NestedDict):
    """The `meta` config for data tests must have the specified keys.

    !!! info "Rationale"

        The `meta` field on data tests is a flexible store for operational metadata such as ownership, severity context, or ticket references. Enforcing required keys ensures that every test carries the information needed to triage failures quickly — for example, knowing which team owns a failing test or what SLA it is tied to — without relying on tribal knowledge or documentation that falls out of sync.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

    Receives:
        test (TestNode): The TestNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the test path. Test paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the test path. Only test paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_test_has_meta_keys
              keys:
                - owner
        ```

    """
    missing_keys = find_missing_meta_keys(
        meta_config=test.meta, required_keys=keys.model_dump()
    )
    if missing_keys:
        fail(
            f"`{test.unique_id}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
        )


@check
def check_test_has_tags(test, *, criteria: Criteria = Criteria.ALL, tags: list[str]):
    """Data tests must have the specified tags.

    !!! info "Rationale"

        Tags allow teams to organise tests into logical groups (e.g. `critical`, `nightly`, `schema-only`) and run subsets selectively with `dbt test --select tag:critical`. Without enforced tagging, it becomes difficult to prioritise test failures, run fast CI checks, or set up tiered alerting based on test severity.

    Parameters:
        criteria (Literal["all", "any", "one"]): Whether the test must have any, all, or exactly one of the specified tags. Default: `all`.
        tags (list[str]): List of tags to check for.

    Receives:
        test (TestNode): The TestNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the test path. Test paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the test path. Only test paths that match any pattern will be checked.
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
    if criteria == Criteria.ANY:
        if not any(tag in resource_tags for tag in tags):
            fail(f"`{test.unique_id}` does not have any of the required tags: {tags}.")
    elif criteria == Criteria.ALL:
        missing_tags = [tag for tag in tags if tag not in resource_tags]
        if missing_tags:
            fail(f"`{test.unique_id}` is missing required tags: {missing_tags}.")
    elif criteria == Criteria.ONE and sum(tag in resource_tags for tag in tags) != 1:
        fail(f"`{test.unique_id}` must have exactly one of the required tags: {tags}.")


@check
def check_test_has_where_config(test, *, regexp_pattern: str | None = None):
    """Data tests must have a `where` config set.

    By default this check only verifies that a `where` config is present (i.e.
    not `None` or empty). Supplying `regexp_pattern` additionally enforces that
    the `where` expression matches the given regex — useful for mandating that a
    specific macro or partition filter is used.

    !!! info "Rationale"

        A `where` config limits a data test to a subset of rows, most commonly a recent time window driven by a partition column. Without one, tests scan the full table on every run, which is slow and expensive on large warehouses, and it is easy to forget to add the filter. Enforcing a `where` config — and optionally that it uses an approved macro — keeps test costs bounded and consistent across a project without relying on manual review.

    !!! warning

        The `where` config is read from the manifest as the **raw Jinja source**, not the compiled SQL. For example, a value of `{{ partition_filter() }} >= 7` is matched exactly as written — dbt-bouncer does not render it, so the environment-specific compiled output (e.g. a 7-day window versus a 1970 epoch window) is never evaluated. Write `regexp_pattern` against the Jinja expression, not the rendered result.

    Parameters:
        regexp_pattern (str | None): If provided, the `where` config must match this regex pattern. The pattern is anchored at the start of the string (`re.match`), so use `.*foo.*` to match anywhere. Default: `None` (only presence is checked).

    Receives:
        test (TestNode): The TestNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the test path. Test paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the test path. Only test paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            # Every data test must set a `where` config.
            - name: check_test_has_where_config
        ```
        ```yaml
        manifest_checks:
            # The `where` config must reference the `partition_filter` macro.
            - name: check_test_has_where_config
              regexp_pattern: .*partition_filter.*
        ```

    """
    where_config = getattr(test.config, "where", None)
    if where_config is None or not str(where_config).strip():
        fail(f"`{test.unique_id}` does not have a `where` config set.")
    elif regexp_pattern is not None and (
        compile_pattern(regexp_pattern.strip()).match(str(where_config)) is None
    ):
        fail(
            f"`{test.unique_id}` has a `where` config that does not match the pattern "
            f"`{regexp_pattern}`: `{where_config}`."
        )
