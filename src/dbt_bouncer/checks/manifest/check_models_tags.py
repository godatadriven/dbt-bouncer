"""Checks related to model tags."""

from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
    )

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import get_clean_model_name


class CheckModelHasTags(BaseCheck):
    """Models must have the specified tags.

    Parameters:
        criteria: (Literal["any", "all", "one"] | None): Whether the model must have any, all, or exactly one of the specified tags. Default: `any`.
        model (DbtBouncerModelBase): The DbtBouncerModelBase object to check.
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

    criteria: Literal["any", "all", "one"] = Field(default="all")
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_has_tags"]
    tags: list[str]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model does not have required tags.

        """
        self._require_model()
        model_tags = self.model.tags or []
        if self.criteria == "any":
            if not any(tag in model_tags for tag in self.tags):
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(self.model.unique_id)}` does not have any of the required tags: {self.tags}."
                )
        elif self.criteria == "all":
            missing_tags = [tag for tag in self.tags if tag not in model_tags]
            if missing_tags:
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(self.model.unique_id)}` is missing required tags: {missing_tags}."
                )
        elif (
            self.criteria == "one" and sum(tag in model_tags for tag in self.tags) != 1
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.model.unique_id)}` must have exactly one of the required tags: {self.tags}."
            )
