"""Backward-compatibility shim — canonical location is ``check_framework.patterns``.

.. deprecated::
    Import from ``dbt_bouncer.check_framework.patterns`` instead.
    This module will be removed in a future major release.
"""

import warnings

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

warnings.warn(
    "Importing from 'dbt_bouncer.check_patterns' is deprecated. "
    "Use 'from dbt_bouncer.check_framework.patterns import ...' instead. "
    "This shim will be removed in a future major release.",
    DeprecationWarning,
    stacklevel=2,
)
