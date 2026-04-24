"""Backward-compatibility shim — canonical location is ``check_framework.exceptions``.

.. deprecated::
    Import from ``dbt_bouncer.check_framework.exceptions`` instead.
    This module will be removed in a future major release.
"""

import warnings

from dbt_bouncer.check_framework.exceptions import (
    DbtBouncerFailedCheckError,
    NestedDict,
)

__all__ = ["DbtBouncerFailedCheckError", "NestedDict"]

warnings.warn(
    "Importing from 'dbt_bouncer.checks.common' is deprecated. "
    "Use 'from dbt_bouncer.check_framework.exceptions import ...' instead. "
    "This shim will be removed in a future major release.",
    DeprecationWarning,
    stacklevel=2,
)
