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


class ResourceType(StrEnum):
    """dbt resource types supported by dbt-bouncer."""

    CATALOG_NODE = auto()
    CATALOG_SOURCE = auto()
    EXPOSURE = auto()
    MACRO = auto()
    MODEL = auto()
    RUN_RESULT = auto()
    SEED = auto()
    SEMANTIC_MODEL = auto()
    SNAPSHOT = auto()
    SOURCE = auto()
    TEST = auto()
    UNIT_TEST = auto()
