import re
from typing import Any, Literal

from pydantic import ConfigDict, Field, PrivateAttr

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.check_patterns import (
    BaseColumnsHaveTypesCheck,
    BaseDescriptionPopulatedCheck,
    BaseHasUnitTestsCheck,
    BaseNamePatternCheck,
)
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


class CheckSeedColumnNames(BaseCheck):
    """Seed columns must have names that match the supplied regex.

    Parameters:
        seed_column_name_pattern (str): Regexp the column name must match.

    Receives:
        seed (SeedNode): The SeedNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_seed_column_names
              seed_column_name_pattern: ^[a-z_]+$  # Lowercase with underscores only
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    name: Literal["check_seed_column_names"]
    seed: Any | None = Field(default=None)
    seed_column_name_pattern: str

    _compiled_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_pattern = compile_pattern(self.seed_column_name_pattern.strip())

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If column name does not match regex.

        """
        seed = self._require_seed()
        seed_columns = seed.columns or {}
        non_complying_columns = [
            col_name
            for col_name in seed_columns
            if self._compiled_pattern.match(str(col_name)) is None
        ]

        if non_complying_columns:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(seed.unique_id)}` has columns that do not match the supplied regex `{self.seed_column_name_pattern.strip()}`: {non_complying_columns}"
            )


class CheckSeedColumnsHaveTypes(BaseColumnsHaveTypesCheck):
    """Columns defined for seeds must have a `data_type` declared.

    Receives:
        seed (SeedNode): The SeedNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_seed_columns_have_types
        ```

    """

    name: Literal["check_seed_columns_have_types"]
    seed: Any | None = Field(default=None)

    @property
    def _resource_columns(self) -> dict[str, Any]:
        return self._require_seed().columns or {}

    @property
    def _resource_display_name(self) -> str:
        return get_clean_model_name(self._require_seed().unique_id)


class CheckSeedDescriptionPopulated(BaseDescriptionPopulatedCheck):
    """Seeds must have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

    Receives:
        seed (SeedNode): The SeedNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_seed_description_populated
        ```
        ```yaml
        manifest_checks:
            - name: check_seed_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    name: Literal["check_seed_description_populated"]
    seed: Any | None = Field(default=None)

    @property
    def _resource_description(self) -> str:
        return self._require_seed().description or ""

    @property
    def _resource_display_name(self) -> str:
        return get_clean_model_name(self._require_seed().unique_id)


class CheckSeedHasUnitTests(BaseHasUnitTestsCheck):
    """Seeds must have more than the specified number of unit tests.

    Parameters:
        min_number_of_unit_tests (int | None): The minimum number of unit tests that a seed must have.

    Receives:
        manifest_obj (ManifestObject): The ManifestObject object parsed from `manifest.json`.
        seed (SeedNode): The SeedNode object to check.
        unit_tests (list[UnitTests]): List of UnitTests objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    !!! warning

        This check is only supported for dbt 1.8.0 and above.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_seed_has_unit_tests
              include: ^seeds/core
        ```
        ```yaml
        manifest_checks:
            - name: check_seed_has_unit_tests
              min_number_of_unit_tests: 2
        ```

    """

    name: Literal["check_seed_has_unit_tests"]
    seed: Any | None = Field(default=None)

    @property
    def _resource_unique_id(self) -> str:
        return self._require_seed().unique_id

    @property
    def _resource_display_name(self) -> str:
        return get_clean_model_name(self._require_seed().unique_id)


class CheckSeedNames(BaseNamePatternCheck):
    """Seed must have a name that matches the supplied regex.

    Parameters:
        seed_name_pattern (str): Regexp the seed name must match.

    Receives:
        seed (SeedNode): The SeedNode object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_seed_names
              include: ^seeds
              model_name_pattern: ^raw_
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    name: Literal["check_seed_names"]
    seed: Any | None = Field(default=None)
    seed_name_pattern: str

    @property
    def _name_pattern(self) -> str:
        return self.seed_name_pattern

    @property
    def _resource_name(self) -> str:
        return str(self._require_seed().name)

    @property
    def _resource_display_name(self) -> str:
        return get_clean_model_name(self._require_seed().unique_id)
