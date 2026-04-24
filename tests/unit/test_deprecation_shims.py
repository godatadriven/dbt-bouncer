"""Tests that backward-compatibility shim modules emit DeprecationWarning."""

import importlib
import sys

import pytest


@pytest.mark.parametrize(
    ("module_path", "expected_symbols"),
    [
        ("dbt_bouncer.check_base", ["BaseCheck"]),
        ("dbt_bouncer.check_context", ["CheckContext"]),
        ("dbt_bouncer.check_decorator", ["check", "fail"]),
        (
            "dbt_bouncer.check_patterns",
            [
                "BaseColumnsHaveTypesCheck",
                "BaseDescriptionPopulatedCheck",
                "BaseHasMetaKeysCheck",
                "BaseHasTagsCheck",
                "BaseHasUnitTestsCheck",
                "BaseNamePatternCheck",
            ],
        ),
        ("dbt_bouncer.checks.common", ["DbtBouncerFailedCheckError", "NestedDict"]),
    ],
    ids=[
        "check_base",
        "check_context",
        "check_decorator",
        "check_patterns",
        "checks.common",
    ],
)
def test_shim_emits_deprecation_warning(
    module_path: str,
    expected_symbols: list[str],
) -> None:
    """Importing a shim module must emit a DeprecationWarning and re-export symbols."""
    # Remove the module from the cache so the import-time warning fires again.
    sys.modules.pop(module_path, None)

    with pytest.warns(DeprecationWarning, match=module_path.replace(".", r"\.")):
        mod = importlib.import_module(module_path)

    for symbol in expected_symbols:
        assert hasattr(mod, symbol), f"{module_path} is missing re-export '{symbol}'"
