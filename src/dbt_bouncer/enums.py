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

    CATALOG_NODE = "catalog_node"
    CATALOG_SOURCE = "catalog_source"
    EXPOSURE = "exposure"
    MACRO = "macro"
    MODEL = "model"
    RUN_RESULT = "run_result"
    SEED = "seed"
    SEMANTIC_MODEL = "semantic_model"
    SNAPSHOT = "snapshot"
    SOURCE = "source"
    TEST = "test"
    UNIT_TEST = "unit_test"
