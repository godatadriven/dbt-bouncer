from typing import TYPE_CHECKING, Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parsers_catalog import CatalogNodes
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        DbtBouncerSeedBase,
    )

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.utils import get_clean_model_name


class CheckSeedColumnsAreAllDocumented(BaseCheck):
    """All columns in a seed CSV file should be included in the seed's properties file, i.e. `.yml` file.

    Receives:
        catalog_node (CatalogNodes): The CatalogNodes object to check.
        manifest_obj (DbtBouncerManifest): The DbtBouncerManifest object parsed from `manifest.json`.
        seeds (list[DbtBouncerSeedBase]): List of DbtBouncerSeedBase objects parsed from `manifest.json`.

    Other Parameters:
        description (str | None): Description of what the check does and why it is implemented.
        exclude (str | None): Regex pattern to match the seed path. Seed paths that match the pattern will not be checked.
        include (str | None): Regex pattern to match the seed path. Only seed paths that match the pattern will be checked.
        severity (Literal["error", "warn"] | None): Severity level of the check. Default: `error`.

    Example(s):
        ```yaml
        catalog_checks:
            - name: check_seed_columns_are_all_documented
        ```

    """

    catalog_node: "CatalogNodes | None" = Field(default=None)
    manifest_obj: "DbtBouncerManifest | None" = Field(default=None)
    name: Literal["check_seed_columns_are_all_documented"]
    seeds: list["DbtBouncerSeedBase"] = Field(default=[])

    def execute(self) -> None:
        """Execute the check.

        Raises:
            DbtBouncerFailedCheckError: If columns are undocumented.

        """
        catalog_node = self._require_catalog_node()
        self._require_manifest()
        if catalog_node.unique_id.startswith("seed."):
            seed = next(s for s in self.seeds if s.unique_id == catalog_node.unique_id)

            seed_columns = seed.columns or {}
            undocumented_columns = [
                v.name
                for _, v in catalog_node.columns.items()
                if v.name not in seed_columns
            ]

            if undocumented_columns:
                raise DbtBouncerFailedCheckError(
                    f"`{get_clean_model_name(seed.unique_id)}` has columns that are not included in the seed properties file: {undocumented_columns}"
                )
