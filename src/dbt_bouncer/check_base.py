"""Backward-compatibility shim — canonical location is ``check_framework.base``.

.. deprecated::
    Import from ``dbt_bouncer.check_framework.base`` instead.
    This module will be removed in a future major release.
"""

import warnings

from dbt_bouncer.check_framework.base import BaseCheck

__all__ = ["BaseCheck"]

warnings.warn(
    "Importing from 'dbt_bouncer.check_base' is deprecated. "
    "Use 'from dbt_bouncer.check_framework.base import BaseCheck' instead. "
    "This shim will be removed in a future major release.",
    DeprecationWarning,
    stacklevel=2,
)
