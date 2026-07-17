"""Package for `dbt-bouncer`."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_bouncer.cli.run.utils import run_bouncer
    from dbt_bouncer.enums import (
        CheckCategory,
        CheckOutcome,
        CheckSeverity,
        ConfigFileName,
        Criteria,
        Materialization,
        ModelAccess,
        ResourceType,
    )

__all__ = [
    "CheckCategory",
    "CheckOutcome",
    "CheckSeverity",
    "ConfigFileName",
    "Criteria",
    "Materialization",
    "ModelAccess",
    "ResourceType",
    "run_bouncer",
]

_ENUM_NAMES = frozenset(
    {
        "CheckCategory",
        "CheckOutcome",
        "CheckSeverity",
        "ConfigFileName",
        "Criteria",
        "Materialization",
        "ModelAccess",
        "ResourceType",
    }
)


def __getattr__(name: str) -> object:
    if name in _ENUM_NAMES:
        from dbt_bouncer import enums

        return getattr(enums, name)
    if name == "run_bouncer":
        from dbt_bouncer.cli.run.utils import run_bouncer

        return run_bouncer
    msg = f"module 'dbt_bouncer' has no attribute {name}"
    raise AttributeError(msg)
