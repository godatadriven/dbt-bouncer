"""Checks related to model column definitions, types, and constraints."""

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
    )

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError, NestedDict
from dbt_bouncer.utils import find_missing_meta_keys, get_clean_model_name


class CheckModelColumnsHaveMetaKeys(BaseCheck):
    """Columns defined for models must have the specified keys in the `meta` config.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_columns_have_meta_keys
              keys:
                - owner
                - pii
        ```

    """

    keys: NestedDict
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_columns_have_meta_keys"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If any model column is missing required meta keys.

        """
        self._require_model()
        columns = self.model.columns or {}
        failing_columns: dict[str, list[str]] = {}
        for col_name, col in columns.items():
            missing_keys = find_missing_meta_keys(
                meta_config=col.meta or {},
                required_keys=self.keys.model_dump(),
            )
            if missing_keys:
                failing_columns[col_name] = [k.replace(">>", "") for k in missing_keys]
        if failing_columns:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has columns missing required `meta` keys: {failing_columns}"
            )


class CheckModelColumnsHaveTypes(BaseCheck):
    """Columns defined for models must have a `data_type` declared.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_columns_have_types
              include: ^models/marts
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_columns_have_types"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If any column lacks a declared `data_type`.

        """
        self._require_model()
        columns = self.model.columns or {}
        untyped_columns = [
            col_name for col_name, col in columns.items() if not col.data_type
        ]
        if untyped_columns:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` has columns without a declared `data_type`: {untyped_columns}"
            )


class CheckModelHasConstraints(BaseCheck):
    """Table and incremental models must have the specified constraint types defined.

    Parameters:
        required_constraint_types (list[Literal["check", "custom", "foreign_key", "not_null", "primary_key", "unique"]]): List of constraint types that must be present on the model.

    Receives:
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_constraints
              required_constraint_types:
                - primary_key
              include: ^models/marts
        ```

    """

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_constraints"]
    required_constraint_types: list[
        Literal["check", "custom", "foreign_key", "not_null", "primary_key", "unique"]
    ]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If required constraint types are missing.

        """
        self._require_model()
        materialization = (
            self.model.config.materialized
            if self.model.config and hasattr(self.model.config, "materialized")
            else None
        )
        if materialization not in ("table", "incremental"):
            return
        constraints = self.model.constraints or []
        actual_types = {
            (c.type.value if hasattr(c.type, "value") else str(c.type))
            for c in constraints
        }
        missing_types = sorted(set(self.required_constraint_types) - actual_types)
        if missing_types:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` is missing required constraint types: {missing_types}"
            )
