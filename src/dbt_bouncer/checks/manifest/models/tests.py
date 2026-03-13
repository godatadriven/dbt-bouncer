"""Checks related to model test coverage and test configuration."""

import logging

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import get_clean_model_name, get_package_version_number


@check
def check_model_has_unique_test(
    model,
    ctx,
    *,
    accepted_uniqueness_tests: list[str] | None = [  # noqa: B006
        "dbt_expectations.expect_compound_columns_to_be_unique",
        "dbt_utils.unique_combination_of_columns",
        "unique",
    ],
):
    """Models must have a test for uniqueness of a column."""
    num_unique_tests = 0
    for test in ctx.tests:
        test_metadata = getattr(test, "test_metadata", None)
        attached_node = getattr(test, "attached_node", None)
        if (
            test_metadata
            and attached_node == model.unique_id
            and (
                (
                    f"{getattr(test_metadata, 'namespace', '')}.{getattr(test_metadata, 'name', '')}"
                    in (accepted_uniqueness_tests or [])
                )
                or (
                    getattr(test_metadata, "namespace", None) is None
                    and getattr(test_metadata, "name", "")
                    in (accepted_uniqueness_tests or [])
                )
            )
        ):
            num_unique_tests += 1
    if num_unique_tests < 1:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` does not have a test for uniqueness of a column."
        )


@check
def check_model_has_unit_tests(model, ctx, *, min_number_of_unit_tests: int = 1):
    """Models must have more than the specified number of unit tests."""
    manifest_obj = ctx.manifest_obj
    if get_package_version_number(
        manifest_obj.manifest.metadata.dbt_version or "0.0.0"
    ) >= get_package_version_number("1.8.0"):
        num_unit_tests = len(
            [
                t.unique_id
                for t in ctx.unit_tests
                if t.depends_on
                and t.depends_on.nodes
                and t.depends_on.nodes[0] == model.unique_id
            ]
        )
        if num_unit_tests < min_number_of_unit_tests:
            display_name = get_clean_model_name(model.unique_id)
            fail(
                f"`{display_name}` has {num_unit_tests} unit tests, this is less than the minimum of {min_number_of_unit_tests}."
            )
    else:
        logging.warning(
            "This unit test check is only supported for dbt 1.8.0 and above."
        )


@check
def check_model_test_coverage(ctx, *, min_model_test_coverage_pct: float = 100):
    """Set the minimum percentage of models that have at least one test."""
    num_models = len(ctx.models)
    # Build set of model IDs that have at least one test
    tested_model_ids = {
        node
        for test in ctx.tests
        if test.depends_on
        for node in (getattr(test.depends_on, "nodes", []) or [])
    }
    model_ids = {m.unique_id for m in ctx.models}
    num_models_with_tests = len(model_ids & tested_model_ids)
    model_test_coverage_pct = (num_models_with_tests / num_models) * 100

    if model_test_coverage_pct < min_model_test_coverage_pct:
        fail(
            f"Only {model_test_coverage_pct}% of models have at least one test, this is less than the permitted minimum of {min_model_test_coverage_pct}%."
        )
