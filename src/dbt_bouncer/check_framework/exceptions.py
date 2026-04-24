"""Exception types and shared Pydantic models for the check framework."""

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


class NestedDict(RootModel):
    """A dictionary that can contain nested dictionaries or lists."""

    root: dict[str, "NestedDict"] | list["NestedDict"] | str
