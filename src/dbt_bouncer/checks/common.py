from pydantic import RootModel


class NestedDict(RootModel):  # type: ignore[type-arg]
    root: dict[str, "NestedDict"] | list["NestedDict"] | str
