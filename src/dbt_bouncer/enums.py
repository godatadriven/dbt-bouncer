from enum import StrEnum, auto


class CheckOutcome(StrEnum):
    """Possible outcomes of a dbt-bouncer check execution."""

    FAILED = "failed"
    SUCCESS = "success"


class CheckSeverity(StrEnum):
    """Severity levels for dbt-bouncer check results."""

    ERROR = "error"
    WARN = "warn"


class ConfigFileName(StrEnum):
    """Config file names recognised by dbt-bouncer."""

    DBT_BOUNCER_TOML = "dbt-bouncer.toml"
    DBT_BOUNCER_YML = "dbt-bouncer.yml"
    PYPROJECT_TOML = "pyproject.toml"


class Materialization(StrEnum):
    """dbt materialization strategies."""

    EPHEMERAL = "ephemeral"
    INCREMENTAL = "incremental"
    TABLE = "table"
    VIEW = "view"


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
