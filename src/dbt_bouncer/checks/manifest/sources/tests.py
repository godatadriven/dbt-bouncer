"""Checks related to source test coverage."""

from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_framework.decorator import check, fail


@check(code="SO016")
def check_source_has_tests(
    source,
    ctx,
    *,
    min_number_of_tests: Annotated[int, Field(gt=0)] = 1,
):
    """Sources must have a minimum number of tests.

    !!! info "Rationale"

        Untested sources are a reliability risk: schema drift or data-quality issues in raw tables can propagate silently through your entire project. Requiring at least one test per source ensures that freshness, uniqueness, or accepted-values constraints are verified before data enters the transformation layer.

    Parameters:
        min_number_of_tests (int): The minimum number of tests a source must have. Default: 1.

    Receives:
        source (SourceNode): The SourceNode object to check.
        tests (list[TestNode]): List of TestNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Source paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the source path (i.e the .yml file where the source is configured). Only source paths that match any pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_source_has_tests
        ```
        ```yaml
        manifest_checks:
            - name: check_source_has_tests
              min_number_of_tests: 2
        ```

    """
    num_tests = len(ctx.tests_by_depends_on_node.get(source.unique_id, []))
    if num_tests < min_number_of_tests:
        fail(
            f"Source `{source.source_name}.{source.name}` has {num_tests} test(s), fewer than the minimum {min_number_of_tests}."
        )
