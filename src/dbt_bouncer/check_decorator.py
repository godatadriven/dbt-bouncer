"""Backward-compatibility shim — canonical location is ``check_framework.decorator``."""

from dbt_bouncer.check_framework.decorator import check, fail

__all__ = ["check", "fail"]
