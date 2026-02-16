"""Resource type enumeration for dbt-bouncer."""

from enum import Enum


class ResourceType(str, Enum):
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
