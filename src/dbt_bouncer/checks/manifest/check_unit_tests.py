import logging
from typing import TYPE_CHECKING, Literal

import pytest
from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.utils import get_package_version_number

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
        UnitTests,
    )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        DbtBouncerModelBase,
    )

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import object_in_path


class CheckUnitTestCoverage(BaseModel):
    """Set the minimum percentage of models that have a unit test.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Parameters:
        min_unit_test_coverage_pct (float): The minimum percentage of models that must have a unit test.

    Receives:
        models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.
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

    model_config = ConfigDict(extra="forbid")
    description: str | None = Field(
        default=None,
        description="Description of what the check does and why it is implemented.",
    )
    include: str | None = Field(
        default=None,
        description="Regexp to match which paths to include.",
    )
    index: int | None = Field(
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    min_unit_test_coverage_pct: int = Field(
        default=100,
        ge=0,
        le=100,
    )
    models: list["DbtBouncerModelBase"] = Field(default=[])
    name: Literal["check_unit_test_coverage"]
    severity: Literal["error", "warn"] | None = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )
    unit_tests: list["UnitTests"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If unit test coverage is less than permitted minimum.

        """
        if self.manifest_obj is None:
            raise DbtBouncerFailedCheckError("self.manifest_obj is None")
        if get_package_version_number(
            self.manifest_obj.manifest.metadata.dbt_version or "0.0.0"
        ) >= get_package_version_number("1.8.0"):
            relevant_models = [
                m.unique_id
                for m in self.models
                if object_in_path(self.include, m.original_file_path)
            ]
            models_with_unit_test = []
            for unit_test in self.unit_tests:
                if unit_test.depends_on and unit_test.depends_on.nodes:
                    for node in unit_test.depends_on.nodes:
                        if node in relevant_models:
                            models_with_unit_test.append(node)

            num_models_with_unit_tests = len(set(models_with_unit_test))
            unit_test_coverage_pct = (
                num_models_with_unit_tests / len(relevant_models)
            ) * 100

            if unit_test_coverage_pct < self.min_unit_test_coverage_pct:
                raise DbtBouncerFailedCheckError(
                    f"Only {unit_test_coverage_pct}% of models have a unit test, this is less than the permitted minimum of {self.min_unit_test_coverage_pct}%."
                )
        else:
            logging.warning(
                "The `check_unit_test_expect_format` check is only supported for dbt 1.8.0 and above.",
            )


class CheckUnitTestExpectFormats(BaseCheck):
    """Unit tests can only use the specified formats.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Parameters:
        permitted_formats (list[Literal["csv", "dict", "sql"]] | None): A list of formats that are allowed to be used for `expect` input in a unit test.

    Receives:
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
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

    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    name: Literal["check_unit_test_expect_format"]
    permitted_formats: list[Literal["csv", "dict", "sql"]] = Field(
        default=["csv", "dict", "sql"],
    )
    unit_test: "UnitTests | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If unit test expect format is not permitted.

        """
        if self.manifest_obj is None:
            raise DbtBouncerFailedCheckError("self.manifest_obj is None")
        if self.unit_test is None:
            raise DbtBouncerFailedCheckError("self.unit_test is None")
        if get_package_version_number(
            self.manifest_obj.manifest.metadata.dbt_version or "0.0.0"
        ) >= get_package_version_number("1.8.0"):
            if self.unit_test.expect.format is None:
                pytest.fail(
                    f"Unit test `{self.unit_test.name}` does not have an `expect` format defined. "
                    f"Permitted formats are: {self.permitted_formats}."
                )

            format_value = (
                self.unit_test.expect.format.value
                if self.unit_test.expect.format
                else None
            )

            if format_value not in self.permitted_formats:
                raise DbtBouncerFailedCheckError(
                    f"Unit test `{self.unit_test.name}` has an `expect` format that is not permitted. "
                    f"Permitted formats are: {self.permitted_formats}. "
                    f"Found: {format_value}"
                )
        else:
            logging.warning(
                "The `check_unit_test_expect_format` check is only supported for dbt 1.8.0 and above.",
            )


class CheckUnitTestGivenFormats(BaseCheck):
    """Unit tests can only use the specified formats.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Parameters:
        permitted_formats (list[Literal["csv", "dict", "sql"]] | None): A list of formats that are allowed to be used for `expect` input in a unit test.

    Receives:
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
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

    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    name: Literal["check_unit_test_given_formats"]
    permitted_formats: list[Literal["csv", "dict", "sql"]] = Field(
        default=["csv", "dict", "sql"],
    )
    unit_test: "UnitTests | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If unit test given formats are not permitted.

        """
        if self.manifest_obj is None:
            raise DbtBouncerFailedCheckError("self.manifest_obj is None")
        if self.unit_test is None:
            raise DbtBouncerFailedCheckError("self.unit_test is None")
        if get_package_version_number(
            self.manifest_obj.manifest.metadata.dbt_version or "0.0.0"
        ) >= get_package_version_number("1.8.0"):
            given_formats = [
                i.format.value for i in self.unit_test.given if i.format is not None
            ]
            if not all(e in self.permitted_formats for e in given_formats):
                raise DbtBouncerFailedCheckError(
                    f"Unit test `{self.unit_test.name}` has given formats which are not permitted. Permitted formats are: {self.permitted_formats}."
                )
        else:
            logging.warning(
                "The `check_unit_test_given_formats` check is only supported for dbt 1.8.0 and above.",
            )
