"""Checks related to model versioning."""

import re
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        DbtBouncerModelBase,
    )

from pydantic import ConfigDict, Field, PrivateAttr

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import compile_pattern


class CheckModelLatestVersionSpecified(BaseCheck):
    r"""Check that the `latest_version` attribute of the model is set.

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
            - name: check_model_latest_version_specified
              include: ^models/marts
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_latest_version_specified"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If latest version is not specified.

        """
        self._require_model()
        if self.model.latest_version is None:
            raise DbtBouncerFailedCheckError(
                f"`{self.model.name}` does not have a specified `latest_version`."
            )


class CheckModelVersionAllowed(BaseCheck):
    r"""Check that the version of the model matches the supplied regex pattern.

    Parameters:
        version_pattern (str): Regexp the version must match.

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
            - name: check_model_version_allowed
              include: ^models/marts
              version_pattern: >- # Versions must be numeric
                [0-9]\d*
            - name: check_model_version_allowed
              include: ^models/marts
              version_pattern: ^(stable|latest)$ # Version can be "stable" or "latest", nothing else is permitted
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_version_allowed"]
    version_pattern: str

    _compiled_pattern: re.Pattern[str] = PrivateAttr()

    def model_post_init(self, __context: object) -> None:
        """Compile the regex pattern once at initialisation time."""
        self._compiled_pattern = compile_pattern(self.version_pattern.strip())

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If version is not allowed.

        """
        self._require_model()
        if self.model.version and (
            self._compiled_pattern.match(str(self.model.version)) is None
        ):
            raise DbtBouncerFailedCheckError(
                f"Version `{self.model.version}` in `{self.model.name}` does not match the supplied regex `{self.version_pattern.strip()})`."
            )


class CheckModelVersionPinnedInRef(BaseCheck):
    r"""Check that the version of the model is always specified in downstream nodes.

    Receives:
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
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
            - name: check_model_version_pinned_in_ref
              include: ^models/marts
        ```

    """

    model_config = ConfigDict(extra="forbid", protected_namespaces=())

    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    model: "DbtBouncerModelBase | None" = Field(default=None)
    name: Literal["check_model_version_pinned_in_ref"]

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If version is not pinned in ref.

        """
        self._require_model()
        self._require_manifest()
        child_map = self.manifest_obj.manifest.child_map
        if child_map and self.model.unique_id in child_map:
            downstream_models = [
                x for x in child_map[self.model.unique_id] if x.startswith("model.")
            ]
        else:
            downstream_models = []

        downstream_models_with_unversioned_refs: list[str] = []
        for m in downstream_models:
            node = self.manifest_obj.manifest.nodes.get(m)
            refs = getattr(node, "refs", None)
            if node and refs and isinstance(refs, list):
                downstream_models_with_unversioned_refs.extend(
                    m
                    for ref in refs
                    if getattr(ref, "name", None) == self.model.unique_id.split(".")[-1]
                    and not getattr(ref, "version", None)
                )

        if downstream_models_with_unversioned_refs:
            raise DbtBouncerFailedCheckError(
                f"`{self.model.name}` is referenced without a pinned version in downstream models: {downstream_models_with_unversioned_refs}."
            )
