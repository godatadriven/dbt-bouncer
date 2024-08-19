from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class BaseCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    include: Optional[str] = Field(
        default=None, description="Regexp to match which paths to include."
    )
    index: Optional[int] = Field(
        default=None, description="Index to uniquely identify the check, calculated at runtime."
    )
