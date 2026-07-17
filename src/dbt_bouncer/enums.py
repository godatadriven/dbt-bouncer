from enum import StrEnum, auto


class CheckOutcome(StrEnum):
    """Possible outcomes of a dbt-bouncer check execution."""

    FAILED = auto()
    SUCCESS = auto()


class CheckCategory(StrEnum):
    """Top-level check categories in a dbt-bouncer config file."""

    CATALOG_CHECKS = auto()
    MANIFEST_CHECKS = auto()
    RUN_RESULTS_CHECKS = auto()

    @property
    def directory(self) -> str:
        """Short check-module subdirectory name (long form minus '_checks')."""
        return self.value.removesuffix("_checks")


class CheckSeverity(StrEnum):
    """Severity levels for dbt-bouncer check results."""

    ERROR = auto()
    WARN = auto()


class ConfigFileName(StrEnum):
    """Config file names recognised by dbt-bouncer."""

    DBT_BOUNCER_TOML = "dbt-bouncer.toml"
    DBT_BOUNCER_YAML = "dbt-bouncer.yaml"
    DBT_BOUNCER_YML = "dbt-bouncer.yml"
    PYPROJECT_TOML = "pyproject.toml"


class ConfigFileSource(StrEnum):
    """Config file names recognised by dbt-bouncer."""

    COMMANDLINE = auto()
    DEFAULT = auto()


class Criteria(StrEnum):
    """How many of a set of required items must match (tags, macros, …)."""

    ALL = auto()
    ANY = auto()
    ONE = auto()


class Materialization(StrEnum):
    """dbt materialization strategies."""

    EPHEMERAL = auto()
    INCREMENTAL = auto()
    TABLE = auto()
    VIEW = auto()


class ModelAccess(StrEnum):
    """dbt model access levels."""

    PRIVATE = auto()
    PROTECTED = auto()
    PUBLIC = auto()


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


class OutputFormatCLI(StrEnum):
    """Supported output formats for CLI list command."""

    JSON = auto()
    TEXT = auto()


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
