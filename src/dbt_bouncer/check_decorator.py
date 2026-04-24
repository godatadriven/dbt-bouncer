"""Backward-compatibility shim — canonical location is ``check_framework.decorator``.

.. deprecated::
    Import from ``dbt_bouncer.check_framework.decorator`` instead.
    This module will be removed in a future major release.
"""

import warnings

from dbt_bouncer.check_framework.decorator import check, fail

__all__ = ["check", "fail"]

warnings.warn(
    "Importing from 'dbt_bouncer.check_decorator' is deprecated. "
    "Use 'from dbt_bouncer.check_framework.decorator import check, fail' instead. "
    "This shim will be removed in a future major release.",
    DeprecationWarning,
    stacklevel=2,
)
