"""Backward-compatibility shim — canonical location is ``check_framework.context``."""

from dbt_bouncer.check_framework.context import CheckContext

__all__ = ["CheckContext"]
