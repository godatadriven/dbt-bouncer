"""Check framework infrastructure for dbt-bouncer.

This package contains the core check infrastructure: base classes, context,
decorator API, reusable abstract patterns, and exception types. Concrete
check implementations live in the sibling ``checks`` package.
"""

from dbt_bouncer.check_framework.base import BaseCheck
from dbt_bouncer.check_framework.context import CheckContext
from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.check_framework.exceptions import (
    DbtBouncerFailedCheckError,
    NestedDict,
)
from dbt_bouncer.check_framework.patterns import (
    BaseColumnsHaveTypesCheck,
    BaseDescriptionPopulatedCheck,
    BaseHasMetaKeysCheck,
    BaseHasTagsCheck,
    BaseHasUnitTestsCheck,
    BaseNamePatternCheck,
)

__all__ = [
    "BaseCheck",
    "BaseColumnsHaveTypesCheck",
    "BaseDescriptionPopulatedCheck",
    "BaseHasMetaKeysCheck",
    "BaseHasTagsCheck",
    "BaseHasUnitTestsCheck",
    "BaseNamePatternCheck",
    "CheckContext",
    "DbtBouncerFailedCheckError",
    "NestedDict",
    "check",
    "fail",
]
