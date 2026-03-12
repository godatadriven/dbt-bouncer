"""Checks related to model naming conventions."""

from typing import Any, Literal

from pydantic import ConfigDict, Field

from dbt_bouncer.check_patterns import BaseNamePatternCheck
from dbt_bouncer.utils import get_clean_model_name


class CheckModelNames(BaseNamePatternCheck):
    """Models must have a name that matches the supplied regex.

    Parameters:
        model_name_pattern (str): Regexp the model name must match.

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
            - name: check_model_names
              include: ^models/intermediate
              model_name_pattern: ^int_
            - name: check_model_names
              include: ^models/staging
              model_name_pattern: ^stg_
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: Any | None = Field(default=None)
    model_name_pattern: str
    name: Literal["check_model_names"]

    @property
    def _name_pattern(self) -> str:
        return self.model_name_pattern

    @property
    def _resource_name(self) -> str:
        return str(self._require_model().name)

    @property
    def _resource_display_name(self) -> str:
        return get_clean_model_name(self._require_model().unique_id)
