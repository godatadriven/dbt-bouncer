import re
from typing import TYPE_CHECKING, Literal

from pydantic import ConfigDict, Field, PrivateAttr

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerSeedBase,
    )

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import compile_pattern, get_clean_model_name


class CheckSeedDescriptionPopulated(BaseCheck):
    """Seeds must have a populated description.

    Parameters:
        min_description_length (int | None): Minimum length required for the description to be considered populated.

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
            - name: check_seed_description_populated
        ```
        ```yaml
        manifest_checks:
            - name: check_seed_description_populated
              min_description_length: 25 # Setting a stricter requirement for description length
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    min_description_length: int | None = Field(default=None)
    name: Literal["check_seed_description_populated"]
    seed: "DbtBouncerSeedBase | None" = Field(default=None)

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If description is not populated.

        """
        self._require_seed()
        if not self._is_description_populated(
            self.seed.description or "", self.min_description_length
        ):
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.seed.unique_id)}` does not have a populated description."
            )


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

    _compiled_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_pattern = compile_pattern(self.seed_name_pattern.strip())

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If model name does not match regex.

        """
        self._require_seed()
        if self._compiled_pattern.match(str(self.seed.name)) is None:
            raise DbtBouncerFailedCheckError(
                f"`{get_clean_model_name(self.seed.unique_id)}` does not match the supplied regex `{self.seed_name_pattern.strip()}`."
            )
