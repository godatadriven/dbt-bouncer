import re
from typing import TYPE_CHECKING, Literal

from pydantic import ConfigDict, Field

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerSeedBase,
    )

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import get_clean_model_name


class CheckSeedNames(BaseCheck):
    """Seed must have a name that matches the supplied regex.

    Parameters:
        seed_name_pattern (str): Regexp the seed name must match.

    Receives:
        seed (DbtBouncerSeedBase): The DbtBouncerSeedBase object to check.

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
    seed: "DbtBouncerSeedBase | None" = Field(default=None)
    seed_name_pattern: str

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model name does not match regex.

        """
        if self.seed is None:
            raise DbtBouncerFailedCheckError("self.seed is None")
        if (
            re.compile(self.seed_name_pattern.strip()).match(str(self.seed.name))
            is None
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.seed.unique_id)}` does not match the supplied regex `{self.seed_name_pattern.strip()}`."
            )
