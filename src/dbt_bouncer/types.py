"""Shared type aliases for dbt-bouncer."""

from typing import Any, TypeAlias

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
