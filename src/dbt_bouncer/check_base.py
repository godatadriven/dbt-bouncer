from typing import TYPE_CHECKING, List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

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

    description: Optional[str] = Field(
        default=None,
        description="Description of what the check does and why it is implemented.",
    )
    exclude: Optional[str] = Field(
        default=None,
        description="Regexp to match which paths to exclude.",
    )
    include: Optional[str] = Field(
        default=None,
        description="Regexp to match which paths to include.",
    )
    index: Optional[int] = Field(
        default=None,
        description="Index to uniquely identify the check, calculated at runtime.",
    )
    severity: Optional[Literal["error", "warn"]] = Field(
        default="error",
        description="Severity of the check, one of 'error' or 'warn'.",
    )

    # Helper methods
    def is_catalog_node_a_model(
        self, catalog_node: "CatalogNodes", models: List["DbtBouncerModelBase"]
    ) -> bool:
        """Check if a catalog node is a model.

        Args:
            catalog_node (CatalogNodes): The CatalogNodes object to check.
            models (List[DbtBouncerModelBase]): List of DbtBouncerModelBase objects parsed from `manifest.json`.

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

    def is_description_populated(self, description: str) -> bool:
        """Check if a description is populated.

        Args:
            description (str): Description.

        Returns:
            bool: Whether a description is validly populated.

        """
        return len(description.strip()) > 4
