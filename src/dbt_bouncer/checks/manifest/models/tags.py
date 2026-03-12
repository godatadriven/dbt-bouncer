"""Checks related to model tags."""

from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_patterns import BaseHasTagsCheck
from dbt_bouncer.utils import get_clean_model_name


class CheckModelHasTags(BaseHasTagsCheck):
    """Models must have the specified tags.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the model must have any, all, or exactly one of the specified tags. Default: `any`.
        model (ModelNode): The ModelNode object to check.
        tags (list[str]): List of tags to check for.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the model path. Model paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the model path. Only model paths that match the pattern will be checked.
        materialization (Literal["ephemeral", "incremental", "table", "view"] | None): Limit check to models with the specified materialization.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        manifest_checks:
            - name: check_model_has_tags
              tags:
                - tag_1
                - tag_2
        ```

    """

    model: Any | None = Field(default=None)
    name: Literal["check_model_has_tags"]

    @property
    def _resource_tags(self) -> list[str]:
        return self._require_model().tags or []

    @property
    def _resource_display_name(self) -> str:
        return get_clean_model_name(self._require_model().unique_id)
