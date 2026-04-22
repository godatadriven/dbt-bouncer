"""Backward-compatibility shim — canonical location is ``check_framework.patterns``."""

from dbt_bouncer.check_framework.patterns import (
    BaseColumnsHaveTypesCheck,
    BaseDescriptionPopulatedCheck,
    BaseHasMetaKeysCheck,
    BaseHasTagsCheck,
    BaseHasUnitTestsCheck,
    BaseNamePatternCheck,
)

__all__ = [
    "BaseColumnsHaveTypesCheck",
    "BaseDescriptionPopulatedCheck",
    "BaseHasMetaKeysCheck",
    "BaseHasTagsCheck",
    "BaseHasUnitTestsCheck",
    "BaseNamePatternCheck",
]
