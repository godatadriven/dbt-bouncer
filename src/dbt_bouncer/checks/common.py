"""Backward-compatibility shim — canonical location is ``check_framework.exceptions``."""

from dbt_bouncer.check_framework.exceptions import (
    DbtBouncerFailedCheckError,
    NestedDict,
)

__all__ = ["DbtBouncerFailedCheckError", "NestedDict"]
