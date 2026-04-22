"""Abstract base classes for common check patterns.

Each ABC provides a template ``execute()`` method and requires subclasses to
implement a small number of abstract properties that supply resource-specific
values (display name, accessor, etc.).
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    import re

from pydantic import Field, PrivateAttr

from dbt_bouncer.check_framework.base import BaseCheck
from dbt_bouncer.check_framework.exceptions import (
    DbtBouncerFailedCheckError,
    NestedDict,
)
from dbt_bouncer.utils import (
    compile_pattern,
    find_missing_meta_keys,
    get_package_version_number,
)


class BaseNamePatternCheck(ABC, BaseCheck):
    """Validate a resource name against a compiled regex pattern.

    Subclasses must define ``_name_pattern``, ``_resource_name``, and
    ``_resource_display_name``.
    """

    _compiled_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_pattern = compile_pattern(self._name_pattern.strip())

    @property
    @abstractmethod
    def _name_pattern(self) -> str:
        """Raw regex pattern string from config."""

    @property
    @abstractmethod
    def _resource_name(self) -> str:
        """Resource name to match against the pattern."""

    @property
    @abstractmethod
    def _resource_display_name(self) -> str:
        """Display name for error messages."""

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If resource name does not match regex.

        """
        if self._compiled_pattern.match(str(self._resource_name)) is None:
            raise DbtBouncerFailedCheckError(
                f"`{self._resource_display_name}` does not match the supplied regex `{self._name_pattern.strip()}`."
            )


class BaseDescriptionPopulatedCheck(ABC, BaseCheck):
    """Validate that a resource has a populated description.

    Subclasses must define ``_resource_description`` and
    ``_resource_display_name``.
    """

    min_description_length: int | None = Field(default=None)

    @property
    @abstractmethod
    def _resource_description(self) -> str:
        """Resource description string."""

    @property
    @abstractmethod
    def _resource_display_name(self) -> str:
        """Display name for error messages."""

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If description is not populated.

        """
        if not self._is_description_populated(
            self._resource_description, self.min_description_length
        ):
            raise DbtBouncerFailedCheckError(
                f"`{self._resource_display_name}` does not have a populated description."
            )


class BaseColumnsHaveTypesCheck(ABC, BaseCheck):
    """Validate that all defined columns have a ``data_type`` declared.

    Subclasses must define ``_resource_columns`` and
    ``_resource_display_name``.
    """

    @property
    @abstractmethod
    def _resource_columns(self) -> dict[str, Any]:
        """Resource columns dict (column_name -> column object)."""

    @property
    @abstractmethod
    def _resource_display_name(self) -> str:
        """Display name for error messages."""

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If any column lacks a declared ``data_type``.

        """
        untyped_columns = [
            col_name
            for col_name, col in self._resource_columns.items()
            if not col.data_type
        ]
        if untyped_columns:
            raise DbtBouncerFailedCheckError(
                f"`{self._resource_display_name}` has columns without a declared `data_type`: {untyped_columns}"
            )


class BaseHasUnitTestsCheck(ABC, BaseCheck):
    """Validate that a resource has a minimum number of unit tests.

    Subclasses must define ``_resource_unique_id`` and
    ``_resource_display_name``.
    """

    min_number_of_unit_tests: int = Field(default=1)

    @property
    @abstractmethod
    def _resource_unique_id(self) -> str:
        """Resource unique_id used to match against unit test dependencies."""

    @property
    @abstractmethod
    def _resource_display_name(self) -> str:
        """Display name for error messages."""

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If resource does not have enough unit tests.

        """
        manifest_obj = self._require_manifest()
        if get_package_version_number(
            manifest_obj.manifest.metadata.dbt_version or "0.0.0"
        ) >= get_package_version_number("1.8.0"):
            num_unit_tests = len(
                [
                    t.unique_id
                    for t in self._ctx.unit_tests
                    if t.depends_on
                    and t.depends_on.nodes
                    and t.depends_on.nodes[0] == self._resource_unique_id
                ],
            )
            if num_unit_tests < self.min_number_of_unit_tests:
                raise DbtBouncerFailedCheckError(
                    f"`{self._resource_display_name}` has {num_unit_tests} unit tests, this is less than the minimum of {self.min_number_of_unit_tests}."
                )
        else:
            logging.warning(
                "This unit test check is only supported for dbt 1.8.0 and above.",
            )


class BaseHasTagsCheck(ABC, BaseCheck):
    """Validate that a resource has specified tags matching criteria.

    Subclasses must define ``_resource_tags`` and ``_resource_display_name``.
    The default ``criteria`` can be overridden per subclass via ``Field(default=...)``.
    """

    criteria: Literal["any", "all", "one"] = Field(default="all")
    tags: list[str]

    @property
    @abstractmethod
    def _resource_tags(self) -> list[str]:
        """Resource tags list."""

    @property
    @abstractmethod
    def _resource_display_name(self) -> str:
        """Display name for error messages."""

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If resource does not have required tags.

        """
        resource_tags = self._resource_tags
        if self.criteria == "any":
            if not any(tag in resource_tags for tag in self.tags):
                raise DbtBouncerFailedCheckError(
                    f"`{self._resource_display_name}` does not have any of the required tags: {self.tags}."
                )
        elif self.criteria == "all":
            missing_tags = [tag for tag in self.tags if tag not in resource_tags]
            if missing_tags:
                raise DbtBouncerFailedCheckError(
                    f"`{self._resource_display_name}` is missing required tags: {missing_tags}."
                )
        elif (
            self.criteria == "one"
            and sum(tag in resource_tags for tag in self.tags) != 1
        ):
            raise DbtBouncerFailedCheckError(
                f"`{self._resource_display_name}` must have exactly one of the required tags: {self.tags}."
            )


class BaseHasMetaKeysCheck(ABC, BaseCheck):
    """Validate that a resource has required keys in its ``meta`` config.

    Subclasses must define ``_resource_meta`` and ``_resource_display_name``.
    """

    keys: NestedDict

    @property
    @abstractmethod
    def _resource_meta(self) -> dict[str, Any]:
        """Resource meta dict."""

    @property
    @abstractmethod
    def _resource_display_name(self) -> str:
        """Display name for error messages."""

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If required meta keys are missing.

        """
        missing_keys = find_missing_meta_keys(
            meta_config=self._resource_meta,
            required_keys=self.keys.model_dump(),
        )
        if missing_keys:
            raise DbtBouncerFailedCheckError(
                f"`{self._resource_display_name}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
            )
