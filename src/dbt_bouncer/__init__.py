"""Package for `dbt-bouncer`."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dbt_bouncer.cli.run.utils import run_bouncer as run_bouncer
    from dbt_bouncer.enums import CheckCategory as CheckCategory
    from dbt_bouncer.enums import CheckOutcome as CheckOutcome
    from dbt_bouncer.enums import CheckSeverity as CheckSeverity
    from dbt_bouncer.enums import ConfigFileName as ConfigFileName
    from dbt_bouncer.enums import Criteria as Criteria
    from dbt_bouncer.enums import Materialization as Materialization
    from dbt_bouncer.enums import ModelAccess as ModelAccess
    from dbt_bouncer.enums import ResourceType as ResourceType

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

# `_ENUM_NAMES` is the single source of truth for which enums are public;
# `__all__` derives from it instead of hand-syncing a second list.
__all__ = sorted({*_ENUM_NAMES, "run_bouncer"})


def __getattr__(name: str) -> object:
    if name in _ENUM_NAMES:
        from dbt_bouncer import enums

        return getattr(enums, name)
    if name == "run_bouncer":
        from dbt_bouncer.cli.run.utils import run_bouncer

        return run_bouncer
    msg = f"module 'dbt_bouncer' has no attribute {name}"
    raise AttributeError(msg)
