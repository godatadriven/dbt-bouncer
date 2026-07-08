"""Checks related to model test coverage and test configuration."""

import logging
from typing import Annotated

from pydantic import Field

from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.utils import get_clean_model_name, get_package_version_number


@check
def check_model_has_tests_by_name(
    model,
    ctx,
    *,
    min_number_of_tests: Annotated[int, Field(gt=0)] = 1,
    test_names: list[str],
):
    """Models must have a minimum number of tests matching the specified test names.

    !!! info "Rationale"

        Some teams require every model to have specific named tests — for example `not_null` on every primary-key column, or a custom `accepted_values` test. This check lets you enumerate the test names that must be present and specify how many matching tests are required.

    Parameters:
        min_number_of_tests (int): The minimum number of tests matching `test_names` a model must have. Default: 1.
        test_names (list[str]): List of test names to count. Generic tests are matched by `test_metadata.name`; singular tests are matched by the test's `name` field.

    Receives:
        model (ModelNode): The ModelNode object to check.
        tests (list[TestNode]): List of TestNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_tests_by_name
              test_names:
                - not_null
                - unique
        ```

    """
    num_matching = 0
    for test in ctx.tests:
        attached_node = getattr(test, "attached_node", None)
        if attached_node != model.unique_id:
            continue
        test_metadata = getattr(test, "test_metadata", None)
        if test_metadata:
            name = getattr(test_metadata, "name", None)
        else:
            name = getattr(test, "name", None)
        if name in test_names:
            num_matching += 1
    if num_matching < min_number_of_tests:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has {num_matching} test(s) matching {test_names}, fewer than the minimum {min_number_of_tests}."
        )


@check
def check_model_has_tests_by_type(
    model,
    ctx,
    *,
    min_number_of_data_tests: Annotated[int, Field(ge=0)] = 0,
    min_number_of_schema_tests: Annotated[int, Field(ge=0)] = 0,
):
    """Models must have at least the specified number of schema tests and data tests.

    !!! info "Rationale"

        dbt tests come in two flavours: schema tests (generic tests declared in .yml files, backed by `test_metadata`) and data tests (singular SQL files with no `test_metadata`). Teams sometimes mandate a minimum number of each type to ensure both broad coverage (via schema tests) and custom business-rule validation (via data tests). This check lets you enforce those minimums per-model.

    Parameters:
        min_number_of_data_tests (int): The minimum number of data (singular) tests a model must have. Default: 0.
        min_number_of_schema_tests (int): The minimum number of schema (generic) tests a model must have. Default: 0.

    Receives:
        model (ModelNode): The ModelNode object to check.
        tests (list[TestNode]): List of TestNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_tests_by_type
              min_number_of_schema_tests: 1
        ```
        ```yaml
        manifest_checks:
            - name: check_model_has_tests_by_type
              min_number_of_data_tests: 1
              min_number_of_schema_tests: 2
        ```

    """
    if min_number_of_data_tests == 0 and min_number_of_schema_tests == 0:
        raise ValueError(
            "At least one of `min_number_of_data_tests` or `min_number_of_schema_tests` must be greater than 0."
        )
    num_schema_tests = 0
    num_data_tests = 0
    for test in ctx.tests:
        test_metadata = getattr(test, "test_metadata", None)
        attached_node = getattr(test, "attached_node", None)
        if attached_node != model.unique_id:
            continue
        if test_metadata:
            num_schema_tests += 1
        else:
            num_data_tests += 1
    failing = []
    if num_schema_tests < min_number_of_schema_tests:
        failing.append(
            f"{num_schema_tests} schema test(s), fewer than the minimum {min_number_of_schema_tests}"
        )
    if num_data_tests < min_number_of_data_tests:
        failing.append(
            f"{num_data_tests} data test(s), fewer than the minimum {min_number_of_data_tests}"
        )
    if failing:
        fail(
            f"`{get_clean_model_name(model.unique_id)}` has "
            + "; and ".join(failing)
            + "."
        )


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
    """Models must have a test for uniqueness of a column.

    !!! info "Rationale"

        A uniqueness test is the most fundamental data quality check — it ensures that the primary key or identifier column of a model does not contain duplicates, which would cause incorrect counts and fan-out in downstream joins. This check enforces that no model reaches production without at least one uniqueness assertion.

    Parameters:
        accepted_uniqueness_tests (list[str] | None): List of tests that are accepted as uniqueness tests.

    Receives:
        model (ModelNode): The ModelNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_unique_test
              include: ^models/marts
        ```
        ```yaml
        manifest_checks:
        # Example of allowing a custom uniqueness test
            - name: check_model_has_unique_test
              accepted_uniqueness_tests:
                - dbt_expectations.expect_compound_columns_to_be_unique # i.e. tests from packages must include package name
                - my_custom_uniqueness_test
                - unique
        ```

    """
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
def check_model_has_unit_tests(
    model, ctx, *, min_number_of_unit_tests: Annotated[int, Field(gt=0)] = 1
):
    """Models must have more than the specified number of unit tests.

    !!! info "Rationale"

        Unit tests validate a model's transformation logic with controlled, mock inputs rather than real warehouse data. Requiring them on critical models (e.g. marts) ensures that complex SQL logic is verified independently of data volume or state, catching regressions before they affect downstream consumers.

    Parameters:
        min_number_of_unit_tests (int | None): The minimum number of unit tests that a model must have.

    Receives:
        manifest_obj (ManifestObject): The ManifestObject object parsed from `manifest.json`.
        model (ModelNode): The ModelNode object to check.
        unit_tests (list[UnitTests]): List of UnitTests objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | list[str] | None): Regex pattern(s) to match the model path. Model paths that match any pattern will not be checked.
        include (str | list[str] | None): Regex pattern(s) to match the model path. Only model paths that match any pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_unit_tests
              include: ^models/marts
        ```
        ```yaml
        manifest_checks:
            - name: check_model_has_unit_tests
              min_number_of_unit_tests: 2
        ```

    """
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
    """Set the minimum percentage of models that have at least one test.

    !!! info "Rationale"

        Rather than requiring every model to be tested immediately, this check lets teams set a realistic coverage target and enforce it progressively. It prevents test coverage from silently declining as new, untested models are added to the project, creating a ratchet towards comprehensive testing.

    Parameters:
        min_model_test_coverage_pct (float): The minimum percentage of models that must have at least one test.

    Receives:
        models (list[ModelNode]): List of ModelNode objects parsed from `manifest.json`.
        tests (list[TestNode]): List of TestNode objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_test_coverage
              min_model_test_coverage_pct: 90
        ```

    """
    if min_model_test_coverage_pct < 0:
        raise ValueError(
            f"`min_model_test_coverage_pct` must be greater than or equal to 0, got {min_model_test_coverage_pct}."
        )
    if min_model_test_coverage_pct > 100:
        raise ValueError(
            f"`min_model_test_coverage_pct` must be less than or equal to 100, got {min_model_test_coverage_pct}."
        )

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
