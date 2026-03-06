"""Checks related to model test coverage and test configuration."""

import logging
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
        UnitTests,
    )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        DbtBouncerModelBase,
        DbtBouncerTestBase,
    )

from pydantic import ConfigDict, Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import get_clean_model_name, get_package_version_number


class CheckModelHasUniqueTest(BaseCheck):
    """Models must have a test for uniqueness of a column.

    Parameters:
        accepted_uniqueness_tests (list[str] | None): List of tests that are accepted as uniqueness tests.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        tests (list[DbtBouncerTestBase]): List of DbtBouncerTestBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
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

    accepted_uniqueness_tests: list[str] | None = Field(
        default=[
            "dbt_expectations.expect_compound_columns_to_be_unique",
            "dbt_utils.unique_combination_of_columns",
            "unique",
        ],
    )
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_unique_test"]
    tests: list["DbtBouncerTestBase"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model does not have a unique test.

        """
        self._require_model()
        num_unique_tests = 0
        for test in self.tests:
            test_metadata = getattr(test, "test_metadata", None)
            attached_node = getattr(test, "attached_node", None)
            if (
                test_metadata
                and attached_node == self.model.unique_id
                and (
                    (
                        f"{getattr(test_metadata, 'namespace', '')}.{getattr(test_metadata, 'name', '')}"
                        in (self.accepted_uniqueness_tests or [])
                    )
                    or (
                        getattr(test_metadata, "namespace", None) is None
                        and getattr(test_metadata, "name", "")
                        in (self.accepted_uniqueness_tests or [])
                    )
                )
            ):
                num_unique_tests += 1
        if num_unique_tests < 1:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` does not have a test for uniqueness of a column."
            )


class CheckModelHasUnitTests(BaseCheck):
    """Models must have more than the specified number of unit tests.

    Parameters:
        min_number_of_unit_tests (int | None): The minimum number of unit tests that a model must have.

    Receives:
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
        unit_tests (list[UnitTests]): List of UnitTests objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
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

    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    min_number_of_unit_tests: int = Field(default=1)
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_unit_tests"]
    unit_tests: list["UnitTests"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model does not have enough unit tests.

        """
        self._require_manifest()
        self._require_model()
        if get_package_version_number(
            self.manifest_obj.manifest.metadata.dbt_version or "0.0.0"
        ) >= get_package_version_number("1.8.0"):
            num_unit_tests = len(
                [
                    t.unique_id
                    for t in self.unit_tests
                    if t.depends_on
                    and t.depends_on.nodes
                    and t.depends_on.nodes[0] == self.model.unique_id
                ],
            )
            if num_unit_tests < self.min_number_of_unit_tests:
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(self.model.unique_id)}` has {num_unit_tests} unit tests, this is less than the minimum of {self.min_number_of_unit_tests}."
                )
        else:
            logging.warning(
                "The `check_model_has_unit_tests` check is only supported for dbt 1.8.0 and above.",
            )


class CheckModelTestCoverage(BaseCheck):
    """Set the minimum percentage of models that have at least one test.

    Parameters:
        min_model_test_coverage_pct (float): The minimum percentage of models that must have at least one test.
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.
        tests (list[DbtBouncerTestBase]): List of DbtBouncerTestBase objects parsed from `manifest.json`.

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

    model_config = ConfigDict(extra="forbid")

    description: str | None = Field(
        default=None,
        description="Description of what the check does and why it is implemented.",
    )
    index: int | None = Field(
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    name: Literal["check_model_test_coverage"]
    min_model_test_coverage_pct: float = Field(
        default=100,
        ge=0,
        le=100,
    )
    models: list["DbtBouncerModelBase"] = Field(default=[])
    severity: Literal["error", "warn"] | None = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )
    tests: list["DbtBouncerTestBase"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If test coverage is less than minimum.

        """
        num_models = len(self.models)
        # Build set of model IDs that have at least one test
        tested_model_ids = {
            node
            for test in self.tests
            if test.depends_on
            for node in (getattr(test.depends_on, "nodes", []) or [])
        }
        model_ids = {m.unique_id for m in self.models}
        num_models_with_tests = len(model_ids & tested_model_ids)
        model_test_coverage_pct = (num_models_with_tests / num_models) * 100

        if model_test_coverage_pct < self.min_model_test_coverage_pct:
            raise DbtBouncerFailedCheckError(
                f"Only {model_test_coverage_pct}% of models have at least one test, this is less than the permitted minimum of {self.min_model_test_coverage_pct}%."
            )
