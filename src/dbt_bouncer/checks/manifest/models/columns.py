"""Checks related to model column definitions, types, and constraints."""

from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.check_patterns import BaseColumnsHaveTypesCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError, NestedDict
from dbt_bouncer.enums import Materialization
from dbt_bouncer.utils import find_missing_meta_keys, get_clean_model_name


class CheckModelColumnsHaveMetaKeys(BaseCheck):
    """Columns defined for models must have the specified keys in the `meta` config.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.

    Receives:
        model (ModelNode): The ModelNode object to check.

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
    model: Any | None = Field(default=None)
    name: Literal["check_model_columns_have_meta_keys"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If any model column is missing required meta keys.

        """
        model = self._require_model()
        columns = model.columns or {}
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
                f"`{get_clean_model_name(model.unique_id)}` has columns missing required `meta` keys: {failing_columns}"
            )


class CheckModelColumnsHaveTypes(BaseColumnsHaveTypesCheck):
    """Columns defined for models must have a `data_type` declared.

    Receives:
        model (ModelNode): The ModelNode object to check.

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

    model: Any | None = Field(default=None)
    name: Literal["check_model_columns_have_types"]

    @property
    def _resource_columns(self) -> dict[str, Any]:
        return self._require_model().columns or {}

    @property
    def _resource_display_name(self) -> str:
        return get_clean_model_name(self._require_model().unique_id)


class CheckModelHasConstraints(BaseCheck):
    """Table and incremental models must have the specified constraint types defined.

    Parameters:
        required_constraint_types (list[Literal["check", "custom", "foreign_key", "not_null", "primary_key", "unique"]]): List of constraint types that must be present on the model.

    Receives:
        model (ModelNode): The ModelNode object to check.

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

    model: Any | None = Field(default=None)
    name: Literal["check_model_has_constraints"]
    required_constraint_types: list[
        Literal["check", "custom", "foreign_key", "not_null", "primary_key", "unique"]
    ]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If required constraint types are missing.

        """
        model = self._require_model()
        materialization = (
            model.config.materialized
            if model.config and hasattr(model.config, "materialized")
            else None
        )
        if materialization not in (Materialization.TABLE, Materialization.INCREMENTAL):
            return
        constraints = model.constraints or []
        actual_types: set[str] = set()
        for c in constraints:
            c_type = getattr(c, "type")  # noqa: B009 - avoids ty shadowing of builtin `type`
            actual_types.add(c_type.value if hasattr(c_type, "value") else str(c_type))
        missing_types = sorted(set(self.required_constraint_types) - actual_types)
        if missing_types:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(model.unique_id)}` is missing required constraint types: {missing_types}"
            )
