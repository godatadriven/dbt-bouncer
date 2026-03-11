"""Checks related to model meta configuration."""

from typing import Any, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError, NestedDict
from dbt_bouncer.utils import find_missing_meta_keys, get_clean_model_name


class CheckModelHasMetaKeys(BaseCheck):
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

    keys: NestedDict
    model: Any | None = Field(default=None)
    name: Literal["check_model_has_meta_keys"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If required meta keys are missing.

        """
        model = self._require_model()
        missing_keys = find_missing_meta_keys(
            meta_config=model.meta,
            required_keys=self.keys.model_dump(),
        )
        if missing_keys != []:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(model.unique_id)}` is missing the following keys from the `meta` config: {[x.replace('>>', '') for x in missing_keys]}"
            )
