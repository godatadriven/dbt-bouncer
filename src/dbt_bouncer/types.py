"""Shared type aliases for dbt-bouncer."""

import re
from typing import Annotated, Any, TypeAlias

from pydantic import AfterValidator

# A resource's ``meta``/``labels`` mapping (``None`` when the config is absent).
MetaConfig: TypeAlias = dict[str, Any] | None

# A single required-key spec: a bare key name, or a ``{key: [sub-keys]}`` mapping
# that requires those sub-keys (recursively) beneath the key. This is the
# plain-data counterpart of the ``NestedDict`` model, i.e. the shape produced by
# ``NestedDict.model_dump()``.
RequiredMetaKey: TypeAlias = "str | dict[str, list[RequiredMetaKey]]"

# The missing keys reported by ``find_missing_meta_keys``. Each entry is a key
# name; nested keys are flattened into a ``>``-joined path (e.g. ``"name>first"``).
MissingMetaKeys: TypeAlias = list[str]


def validate_regex_pattern(value: str) -> str:
    """Reject a string that does not compile as a regular expression.

    Checks strip patterns before compiling them, so the stripped form is what
    is validated. The value itself is returned unchanged.

    Returns:
        str: The unchanged pattern.

    Raises:
        ValueError: If the pattern is not a valid regular expression. Pydantic
            reports it as a validation error, so an invalid pattern stops the
            run at config load rather than crashing once per resource.

    """
    try:
        re.compile(value.strip())
    except re.error as e:
        raise ValueError(f"Invalid regex pattern '{value.strip()}': {e}") from e
    return value


def validate_regex_patterns(value: str | list[str] | None) -> str | list[str] | None:
    """Validate one pattern or a list of patterns, as ``include``/``exclude`` accept.

    Returns:
        str | list[str] | None: The unchanged value.

    """
    if value is None:
        return None
    for pattern in [value] if isinstance(value, str) else value:
        validate_regex_pattern(pattern)
    return value


# A check parameter holding a regular expression. Validated when the config is
# loaded, so a typo is a config error (exit code 2) instead of a crash in every
# check run.
RegexPattern: TypeAlias = Annotated[str, AfterValidator(validate_regex_pattern)]
