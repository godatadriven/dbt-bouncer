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


class PropertiesLayout(StrEnum):
    """Layouts for model properties (`.yml`) files."""

    PER_DIRECTORY = auto()
    PER_MODEL = auto()


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


class CatalogRuleCode(StrEnum):
    """Rule codes for catalog checks."""

    CHECK_COLUMN_DESCRIPTION_POPULATED = "CA005"
    CHECK_COLUMN_HAS_SPECIFIED_TEST = "CA011"
    CHECK_COLUMN_NAME_COMPLIES_TO_COLUMN_TYPE = "CA008"
    CHECK_COLUMN_NAMES = "CA009"
    CHECK_COLUMN_TYPE_COMPLIES_TO_COLUMN_NAME = "CA010"
    CHECK_COLUMNS_ARE_ALL_DOCUMENTED = "CA006"
    CHECK_COLUMNS_ARE_DOCUMENTED_IN_PUBLIC_MODELS = "CA007"
    CHECK_SEED_COLUMNS_ARE_ALL_DOCUMENTED = "CA001"
    CHECK_SEED_MAX_BYTES = "CA002"
    CHECK_SEED_MAX_ROW_COUNT = "CA003"
    CHECK_SOURCE_COLUMNS_ARE_ALL_DOCUMENTED = "CA004"


class ExposureRuleCode(StrEnum):
    """Rule codes for exposure checks."""

    CHECK_EXPOSURE_BASED_ON_MODEL = "EX001"
    CHECK_EXPOSURE_BASED_ON_NON_PUBLIC_MODELS = "EX002"
    CHECK_EXPOSURE_BASED_ON_VIEW = "EX003"
    CHECK_EXPOSURE_DESCRIPTION_POPULATED = "EX004"
    CHECK_EXPOSURE_HAS_META_KEYS = "EX005"
    CHECK_EXPOSURE_HAS_OWNER = "EX006"


class LineageRuleCode(StrEnum):
    """Rule codes for lineage checks."""

    CHECK_LINEAGE_PERMITTED_UPSTREAM_MODELS = "LI001"
    CHECK_LINEAGE_SEED_CANNOT_BE_USED = "LI002"
    CHECK_LINEAGE_SOURCE_CANNOT_BE_USED = "LI003"


class MacroRuleCode(StrEnum):
    """Rule codes for macro checks."""

    CHECK_MACRO_ARGUMENTS_DESCRIPTION_POPULATED = "MA001"
    CHECK_MACRO_CODE_DOES_NOT_CONTAIN_REGEXP_PATTERN = "MA002"
    CHECK_MACRO_DESCRIPTION_POPULATED = "MA003"
    CHECK_MACRO_HAS_META_KEYS = "MA004"
    CHECK_MACRO_IS_USED = "MA005"
    CHECK_MACRO_MAX_NUMBER_OF_ARGUMENTS = "MA006"
    CHECK_MACRO_MAX_NUMBER_OF_LINES = "MA007"
    CHECK_MACRO_NAME_MATCHES_FILE_NAME = "MA008"
    CHECK_MACRO_NAMES = "MA009"
    CHECK_MACRO_PROPERTY_FILE_LOCATION = "MA010"


class MetadataRuleCode(StrEnum):
    """Rule codes for metadata checks."""

    CHECK_PROJECT_NAME = "ME001"


class ModelRuleCode(StrEnum):
    """Rule codes for model checks."""

    CHECK_COLUMN_DESCRIPTIONS_ARE_CONSISTENT = "MO019"
    CHECK_MODEL_ACCESS = "MO001"
    CHECK_MODEL_CODE_DOES_NOT_CONTAIN_REGEXP_PATTERN = "MO007"
    CHECK_MODEL_COLUMNS_HAVE_META_KEYS = "MO014"
    CHECK_MODEL_COLUMNS_HAVE_RELATIONSHIP_TESTS = "MO015"
    CHECK_MODEL_COLUMNS_HAVE_TYPES = "MO016"
    CHECK_MODEL_CONTRACT_ENFORCED_FOR_PUBLIC_MODEL = "MO002"
    CHECK_MODEL_DEPENDS_ON_MACROS = "MO028"
    CHECK_MODEL_DEPENDS_ON_MULTIPLE_SOURCES = "MO029"
    CHECK_MODEL_DESCRIPTION_CONTAINS_REGEX_PATTERN = "MO020"
    CHECK_MODEL_DESCRIPTION_POPULATED = "MO021"
    CHECK_MODEL_DIRECTORIES = "MO024"
    CHECK_MODEL_DOCUMENTATION_COVERAGE = "MO022"
    CHECK_MODEL_DOCUMENTED_IN_SAME_DIRECTORY = "MO023"
    CHECK_MODEL_DOES_NOT_DIRECTLY_JOIN_TO_SOURCE = "MO048"
    CHECK_MODEL_DOES_NOT_REJOIN_UPSTREAM_CONCEPTS = "MO049"
    CHECK_MODEL_DOES_NOT_USE_SELECT_STAR = "MO008"
    CHECK_MODEL_FILE_NAME = "MO025"
    CHECK_MODEL_GRANT_PRIVILEGE = "MO003"
    CHECK_MODEL_GRANT_PRIVILEGE_REQUIRED = "MO004"
    CHECK_MODEL_HARD_CODED_REFERENCES = "MO009"
    CHECK_MODEL_HAS_CONSTRAINTS = "MO017"
    CHECK_MODEL_HAS_CONTRACTS_ENFORCED = "MO005"
    CHECK_MODEL_HAS_EXPOSURE = "MO030"
    CHECK_MODEL_HAS_LABELS_KEYS = "MO036"
    CHECK_MODEL_HAS_META_KEYS = "MO037"
    CHECK_MODEL_HAS_NO_UPSTREAM_DEPENDENCIES = "MO031"
    CHECK_MODEL_HAS_PROPERTIES_FILE = "MO051"
    CHECK_MODEL_HAS_SEMI_COLON = "MO010"
    CHECK_MODEL_HAS_TAGS = "MO039"
    CHECK_MODEL_HAS_TESTS_BY_NAME = "MO040"
    CHECK_MODEL_HAS_TESTS_BY_TYPE = "MO041"
    CHECK_MODEL_HAS_UNIQUE_TEST = "MO042"
    CHECK_MODEL_HAS_UNIT_TESTS = "MO043"
    CHECK_MODEL_INCREMENTAL_HAS_UNIQUE_KEY = "MO011"
    CHECK_MODEL_LATEST_VERSION_SPECIFIED = "MO045"
    CHECK_MODEL_MATERIALIZATION_BY_FANOUT = "MO032"
    CHECK_MODEL_MATERIALIZATION_PERMITTED = "MO012"
    CHECK_MODEL_MAX_CHAINED_VIEWS = "MO033"
    CHECK_MODEL_MAX_FANOUT = "MO034"
    CHECK_MODEL_MAX_NUMBER_OF_LINES = "MO013"
    CHECK_MODEL_MAX_UPSTREAM_DEPENDENCIES = "MO035"
    CHECK_MODEL_MIN_DOWNSTREAM_MODELS = "MO050"
    CHECK_MODEL_NAMES = "MO038"
    CHECK_MODEL_NUMBER_OF_GRANTS = "MO006"
    CHECK_MODEL_PROPERTY_FILE_LOCATION = "MO026"
    CHECK_MODEL_SCHEMA_NAME = "MO027"
    CHECK_MODEL_SINGLE_PRIMARY_KEY = "MO018"
    CHECK_MODEL_TEST_COVERAGE = "MO044"
    CHECK_MODEL_VERSION_ALLOWED = "MO046"
    CHECK_MODEL_VERSION_PINNED_IN_REF = "MO047"


class RunResultsRuleCode(StrEnum):
    """Rule codes for run_results checks."""

    CHECK_RUN_RESULTS_MAX_EXECUTION_TIME = "RR001"
    CHECK_RUN_RESULTS_MAX_GIGABYTES_BILLED = "RR002"


class SeedRuleCode(StrEnum):
    """Rule codes for seed checks."""

    CHECK_SEED_COLUMN_NAMES = "SE001"
    CHECK_SEED_COLUMNS_HAVE_TYPES = "SE002"
    CHECK_SEED_DESCRIPTION_POPULATED = "SE003"
    CHECK_SEED_HAS_META_KEYS = "SE004"
    CHECK_SEED_HAS_UNIT_TESTS = "SE005"
    CHECK_SEED_NAMES = "SE006"


class SemanticModelRuleCode(StrEnum):
    """Rule codes for semantic_model checks."""

    CHECK_SEMANTIC_MODEL_BASED_ON_NON_PUBLIC_MODELS = "SM001"


class SnapshotRuleCode(StrEnum):
    """Rule codes for snapshot checks."""

    CHECK_SNAPSHOT_DESCRIPTION_POPULATED = "SN001"
    CHECK_SNAPSHOT_HAS_META_KEYS = "SN002"
    CHECK_SNAPSHOT_HAS_TAGS = "SN003"
    CHECK_SNAPSHOT_HAS_UNIQUE_KEY = "SN004"
    CHECK_SNAPSHOT_NAMES = "SN005"
    CHECK_SNAPSHOT_STRATEGY = "SN006"


class SourceRuleCode(StrEnum):
    """Rule codes for source checks."""

    CHECK_DUPLICATE_SOURCES = "SO005"
    CHECK_SOURCE_DESCRIPTION_POPULATED = "SO001"
    CHECK_SOURCE_FILE_NAME = "SO002"
    CHECK_SOURCE_FRESHNESS_POPULATED = "SO004"
    CHECK_SOURCE_HAS_LABELS_KEYS = "SO011"
    CHECK_SOURCE_HAS_META_KEYS = "SO012"
    CHECK_SOURCE_HAS_TAGS = "SO015"
    CHECK_SOURCE_HAS_TESTS = "SO016"
    CHECK_SOURCE_LOADER_POPULATED = "SO010"
    CHECK_SOURCE_MIN_DOWNSTREAM_MODELS = "SO006"
    CHECK_SOURCE_NAMES = "SO014"
    CHECK_SOURCE_NOT_ORPHANED = "SO007"
    CHECK_SOURCE_PII_META = "SO013"
    CHECK_SOURCE_PROPERTY_FILE_LOCATION = "SO003"
    CHECK_SOURCE_TOP_LEVEL_DESCRIPTION_POPULATED = "SO017"
    CHECK_SOURCE_USED_BY_MODELS_IN_SAME_DIRECTORY = "SO008"
    CHECK_SOURCE_USED_BY_ONLY_ONE_MODEL = "SO009"


class TestRuleCode(StrEnum):
    """Rule codes for test checks."""

    CHECK_TEST_HAS_META_KEYS = "TE001"
    CHECK_TEST_HAS_TAGS = "TE002"
    CHECK_TEST_HAS_WHERE_CONFIG = "TE003"


class UnitTestRuleCode(StrEnum):
    """Rule codes for unit_test checks."""

    CHECK_UNIT_TEST_COVERAGE = "UT001"
    CHECK_UNIT_TEST_EXPECT_FORMAT = "UT002"
    CHECK_UNIT_TEST_GIVEN_FORMATS = "UT003"


RuleCode = (
    CatalogRuleCode
    | ExposureRuleCode
    | LineageRuleCode
    | MacroRuleCode
    | MetadataRuleCode
    | ModelRuleCode
    | RunResultsRuleCode
    | SeedRuleCode
    | SemanticModelRuleCode
    | SnapshotRuleCode
    | SourceRuleCode
    | TestRuleCode
    | UnitTestRuleCode
)
