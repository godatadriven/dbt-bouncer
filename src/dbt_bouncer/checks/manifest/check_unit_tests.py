"""Checks related to unit test coverage and formats."""

import logging

from dbt_bouncer.check_decorator import check, fail
from dbt_bouncer.utils import get_package_version_number, object_in_path


@check
def check_unit_test_coverage(
    ctx, *, include: str | None = None, min_unit_test_coverage_pct: int = 100
):
    """Set the minimum percentage of models that have a unit test."""
    manifest_obj = ctx.manifest_obj
    if get_package_version_number(
        manifest_obj.manifest.metadata.dbt_version or "0.0.0"
    ) >= get_package_version_number("1.8.0"):
        relevant_models = [
            m.unique_id
            for m in ctx.models
            if object_in_path(include, m.original_file_path)
        ]
        models_with_unit_test = []
        for unit_test in ctx.unit_tests:
            if unit_test.depends_on and unit_test.depends_on.nodes:
                for node in unit_test.depends_on.nodes:
                    if node in relevant_models:
                        models_with_unit_test.append(node)

        num_models_with_unit_tests = len(set(models_with_unit_test))
        unit_test_coverage_pct = (
            num_models_with_unit_tests / len(relevant_models)
        ) * 100

        if unit_test_coverage_pct < min_unit_test_coverage_pct:
            fail(
                f"Only {unit_test_coverage_pct}% of models have a unit test, this is less than the permitted minimum of {min_unit_test_coverage_pct}%."
            )
    else:
        logging.warning(
            "The `check_unit_test_expect_format` check is only supported for dbt 1.8.0 and above."
        )


@check
def check_unit_test_expect_format(
    unit_test,
    ctx,
    *,
    permitted_formats: list[str] = ["csv", "dict", "sql"],  # noqa: B006
):
    """Unit tests can only use the specified formats."""
    manifest_obj = ctx.manifest_obj
    if get_package_version_number(
        manifest_obj.manifest.metadata.dbt_version or "0.0.0"
    ) >= get_package_version_number("1.8.0"):
        if unit_test.expect.format is None:
            fail(
                f"Unit test `{unit_test.name}` does not have an `expect` format defined. "
                f"Permitted formats are: {permitted_formats}."
            )

        format_value = (
            unit_test.expect.format.value if unit_test.expect.format else None
        )

        if format_value not in permitted_formats:
            fail(
                f"Unit test `{unit_test.name}` has an `expect` format that is not permitted. "
                f"Permitted formats are: {permitted_formats}. "
                f"Found: {format_value}"
            )
    else:
        logging.warning(
            "The `check_unit_test_expect_format` check is only supported for dbt 1.8.0 and above."
        )


@check
def check_unit_test_given_formats(
    unit_test,
    ctx,
    *,
    permitted_formats: list[str] = ["csv", "dict", "sql"],  # noqa: B006
):
    """Unit tests can only use the specified formats."""
    manifest_obj = ctx.manifest_obj
    if get_package_version_number(
        manifest_obj.manifest.metadata.dbt_version or "0.0.0"
    ) >= get_package_version_number("1.8.0"):
        given_formats = [
            i.format.value for i in unit_test.given if i.format is not None
        ]
        if not all(e in permitted_formats for e in given_formats):
            fail(
                f"Unit test `{unit_test.name}` has given formats which are not permitted. Permitted formats are: {permitted_formats}."
            )
    else:
        logging.warning(
            "The `check_unit_test_given_formats` check is only supported for dbt 1.8.0 and above."
        )
