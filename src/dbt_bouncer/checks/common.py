from typing import Dict, List, Union

from pydantic import RootModel


class NestedDict(RootModel):  # type: ignore[type-arg]
    root: Union[Dict[str, "NestedDict"], List["NestedDict"], str]
