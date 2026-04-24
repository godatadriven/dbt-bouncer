"""Backward-compatibility shim — canonical location is ``check_framework.context``.

.. deprecated::
    Import from ``dbt_bouncer.check_framework.context`` instead.
    This module will be removed in a future major release.
"""

import warnings

from dbt_bouncer.check_framework.context import CheckContext

__all__ = ["CheckContext"]

warnings.warn(
    "Importing from 'dbt_bouncer.check_context' is deprecated. "
    "Use 'from dbt_bouncer.check_framework.context import CheckContext' instead. "
    "This shim will be removed in a future major release.",
    DeprecationWarning,
    stacklevel=2,
)
