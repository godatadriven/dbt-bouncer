from pydantic import RootModel


class DbtBouncerFailedCheckError(Exception):
    """A custom exception class for failing dbt-bouncer checks."""

    def __init__(self, message: str):
        """Initialize the DbtBouncerFailedCheck exception.

        Args:
            message (str): The exception message.

        """
        self.message = message
        super().__init__(self.message)

    def __str__(self) -> str:
        """Return the string representation of the exception.

        Returns:
            str: The exception message.

        """
        return self.message


class NestedDict(RootModel):  # type: ignore[type-arg]
    root: dict[str, "NestedDict"] | list["NestedDict"] | str
