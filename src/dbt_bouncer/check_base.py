from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


class BaseCheck(BaseModel):
    """Base class for all checks."""

    model_config = ConfigDict(extra="forbid")

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
