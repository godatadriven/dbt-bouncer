"""Checks related to unit test coverage and formats."""

import logging
from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import get_package_version_number, object_in_path


@check
def check_unit_test_coverage(
    ctx,
    *,
    include: str | None = None,
    min_unit_test_coverage_pct: Annotated[int, Field(ge=0, le=100)] = 100,
):
    """Set the minimum percentage of models that have a unit test.

    !!! info "Rationale"

        Unit tests validate that a model's SQL logic produces the correct output for a given set of inputs, independently of live data. Tracking coverage across the project ensures that critical business logic is not left untested, which reduces the risk of silent regressions when models are refactored or when source data shapes change unexpectedly.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Parameters:
        min_unit_test_coverage_pct (float): The minimum percentage of models that must have a unit test.

    Receives:
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.
        unit_tests (list[UnitTests]): List of UnitTests objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_unit_test_coverage
              min_unit_test_coverage_pct: 90
        ```

    """
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
    """Unit tests can only use the specified formats.

    !!! info "Rationale"

        dbt unit tests support multiple `expect` formats — `csv`, `dict`, and `sql` — each with different trade-offs in readability, portability, and maintenance overhead. Restricting permitted formats enforces a project-wide standard that keeps unit tests consistent and readable, and avoids formats that may be harder for the team to maintain (e.g. raw SQL expectations that are difficult to diff).

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Parameters:
        permitted_formats (list[Literal["csv", "dict", "sql"]] | None): A list of formats that are allowed to be used for `expect` input in a unit test.

    Receives:
        manifest_obj (ManifestObject): The ManifestObject object parsed from `manifest.json`.
        unit_test (UnitTests): The UnitTests object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Unit test paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Only unit test paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_unit_test_expect_format
              permitted_formats:
                - csv
        ```

    """
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
    """Unit tests can only use the specified formats.

    !!! info "Rationale"

        The `given` section of a dbt unit test defines the mock input data for the test. Standardising the format across all `given` inputs (e.g. requiring `csv` for human-readable diffs in code review) keeps tests consistent and ensures that engineers reviewing test changes can quickly assess whether the input data is correct without needing to parse unfamiliar formats.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Parameters:
        permitted_formats (list[Literal["csv", "dict", "sql"]] | None): A list of formats that are allowed to be used for `expect` input in a unit test.

    Receives:
        manifest_obj (ManifestObject): The ManifestObject object parsed from `manifest.json`.
        unit_test (UnitTests): The UnitTests object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Unit test paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the unit test path (i.e the .yml file where the unit test is configured). Only unit test paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_unit_test_given_formats
              permitted_formats:
                - csv
        ```

    """
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
