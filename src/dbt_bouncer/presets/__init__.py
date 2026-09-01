"""Bundled configuration presets shipped with dbt-bouncer.

A preset is a ready-to-run config file. Use a preset for a zero-config run
(`dbt-bouncer run --preset strict`) or scaffold an editable copy
(`dbt-bouncer init --preset strict`).
"""

from __future__ import annotations

from importlib import resources
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dbt_bouncer.enums import PresetName


def read_preset_text(name: PresetName | str) -> str:
    """Return the raw text of a bundled preset config file.

    Args:
        name: The preset name (`minimal`, `standard`, or `strict`).

    Returns:
        str: The preset file text, comments included.

    """
    resource = resources.files(__name__) / f"{name}.yml"
    return resource.read_text(encoding="utf-8")


def load_preset_contents(name: PresetName | str) -> dict[str, Any]:
    """Load and parse a bundled preset config file.

    Args:
        name: The preset name (`minimal`, `standard`, or `strict`).

    Returns:
        dict[str, Any]: The parsed preset config contents.

    """
    import yaml

    return yaml.load(read_preset_text(name), Loader=yaml.CSafeLoader)  # type: ignore[possibly-missing-attribute]
