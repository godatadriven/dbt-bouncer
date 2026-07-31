"""Tests for the lazy subcommand loader in `dbt_bouncer.cli.__init__`."""

import re
from types import ModuleType

import pytest

import dbt_bouncer.cli

# `init`, `run` and `validate` are submodule names as well as subcommand
# function names. Importing a submodule binds it onto the parent package, and a
# module-level `__getattr__` is only consulted after normal attribute lookup
# fails -- so those three can never be re-exported reliably. `list_checks` is
# the one subcommand whose name no submodule shares (the module is `list`).
SHADOWED_BY_SUBMODULES = ["init", "run", "validate"]


def test_resolves_list_checks_to_a_callable() -> None:
    """`list_checks` is the one subcommand the loader can resolve reliably."""
    resolved = dbt_bouncer.cli.__getattr__("list_checks")

    assert callable(resolved)
    assert resolved.__name__ == "list_checks"


def test_list_checks_is_not_shadowed_by_a_submodule() -> None:
    """Attribute access reaches the loader, so `list_checks` is the function rather than a module."""
    assert callable(dbt_bouncer.cli.list_checks)
    assert not isinstance(dbt_bouncer.cli.list_checks, ModuleType)


@pytest.mark.parametrize("name", SHADOWED_BY_SUBMODULES)
def test_submodule_names_are_not_re_exported(name: str) -> None:
    """The loader refuses the three names a submodule shadows, rather than returning an import-order-dependent result."""
    with pytest.raises(
        AttributeError,
        match=re.escape(f"module 'dbt_bouncer.cli' has no attribute {name!r}"),
    ):
        dbt_bouncer.cli.__getattr__(name)


@pytest.mark.parametrize("name", SHADOWED_BY_SUBMODULES)
def test_submodule_names_resolve_to_their_modules(name: str) -> None:
    """Attribute access for those names yields the submodule, which is what the import system binds."""
    # `main` eagerly imports every subcommand module to trigger registration.
    import dbt_bouncer.main

    assert isinstance(getattr(dbt_bouncer.cli, name), ModuleType)


@pytest.mark.parametrize(
    "name",
    [
        pytest.param("does_not_exist", id="unknown_name"),
        # `list` is the module that holds `list_checks`; it is not itself an
        # exported subcommand.
        pytest.param("list", id="submodule_name_is_not_an_export"),
        pytest.param("app", id="module_level_attribute_never_reaches_getattr"),
        pytest.param("", id="empty_string"),
    ],
)
def test_unsupported_name_raises_attribute_error(name: str) -> None:
    """An unsupported name raises AttributeError rather than returning None."""
    with pytest.raises(
        AttributeError,
        match=re.escape(f"module 'dbt_bouncer.cli' has no attribute {name!r}"),
    ):
        dbt_bouncer.cli.__getattr__(name)


def test_all_lists_only_what_is_actually_reachable() -> None:
    """`__all__` stays in step with what the loader can resolve, and claims nothing it cannot."""
    assert dbt_bouncer.cli.__all__ == ["app", "list_checks"]


def test_app_is_a_real_attribute_not_lazily_loaded() -> None:
    """`app` is bound at import time, so attribute access never reaches the loader."""
    assert dbt_bouncer.cli.app is not None
    assert "app" in vars(dbt_bouncer.cli)
