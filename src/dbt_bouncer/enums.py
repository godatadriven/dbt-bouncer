from enum import StrEnum, auto


class OutputFormat(StrEnum):
    """Supported output formats for CLI and programmatic output."""

    CSV = auto()
    JSON = auto()
    JUNIT = auto()
    SARIF = auto()
    TAP = auto()

    @classmethod
    def values(cls) -> list:
        """Return all output format values as lowercase strings."""  # noqa: DOC201
        return list(cls)
