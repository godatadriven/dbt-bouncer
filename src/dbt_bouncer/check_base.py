from typing import TYPE_CHECKING, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field

from dbt_bouncer.utils import is_description_populated

if TYPE_CHECKING:
    import warnings

    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        from dbt_artifacts_parser.parsers.catalog.catalog_v1 import (
            Nodes as CatalogNodes,
        )
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerModelBase,
    )


class BaseCheck(BaseModel):
    """Base class for all checks."""

    model_config = ConfigDict(extra="forbid")

    description: str | None = Field(
        default=None,
        description="Description of what the check does and why it is implemented.",
    )
    exclude: str | None = Field(
        default=None,
        description="Regexp to match which paths to exclude.",
    )
    include: str | None = Field(
        default=None,
        description="Regexp to match which paths to include.",
    )
    index: int | None = Field(
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    materialization: Literal["ephemeral", "incremental", "table", "view"] | None = (
        Field(
            default=None,
            description="Limit check to models with the specified materialization.",
        )
    )
    severity: Literal["error", "warn"] | None = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )

    _min_description_length: ClassVar[int] = 4

    # Helper methods
    def is_catalog_node_a_model(
        self, catalog_node: "CatalogNodes", models: list["DbtBouncerModelBase"]
    ) -> bool:
        """Check if a catalog node is a model.

        Args:
            catalog_node (CatalogNodes): The CatalogNodes object to check.
            models (list[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

        Returns:
            bool: Whether a catalog node is a model.

        """
        catalog_node_model = [
            m for m in models if m.unique_id == catalog_node.unique_id
        ]
        if catalog_node_model:
            return catalog_node_model[0].resource_type == "model"
        else:
            return False

    def _is_description_populated(
        self, description: str, min_description_length: int | None
    ) -> bool:
        """Check if a description is populated.

        Args:
            description (str): Description.
            min_description_length (int): Minimum length of the description.

        Returns:
            bool: Whether a description is validly populated.

        """
        return is_description_populated(
            description=description,
            min_description_length=min_description_length
            or self._min_description_length,
        )
