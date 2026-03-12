"""Checks related to model meta configuration."""

from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_patterns import BaseHasMetaKeysCheck
from dbt_bouncer.utils import get_clean_model_name


class CheckModelHasMetaKeys(BaseHasMetaKeysCheck):
    """The `meta` config for models must have the specified keys.

    Parameters:
        keys (NestedDict): A list (that may contain sub-lists) of required keys.
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
            - name: check_model_has_meta_keys
              keys:
                - maturity
                - owner
        ```

    """

    model: Any | None = Field(default=None)
    name: Literal["check_model_has_meta_keys"]

    @property
    def _resource_meta(self) -> dict[str, Any]:
        return self._require_model().meta

    @property
    def _resource_display_name(self) -> str:
        return get_clean_model_name(self._require_model().unique_id)
