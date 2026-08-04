"""Check framework infrastructure for dbt-bouncer.

This package contains the core check infrastructure: base classes, context,
decorator API, and exception types. Concrete check implementations live in
the sibling ``checks`` package.
"""

from dbt_bouncer.check_framework.base import BaseCheck
from dbt_bouncer.check_framework.context import CheckContext
from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.check_framework.exceptions import (
    DbtBouncerFailedCheckError,
    NestedDict,
)

__all__ = [
    "BaseCheck",
    "CheckContext",
    "DbtBouncerFailedCheckError",
    "NestedDict",
    "check",
    "fail",
]
