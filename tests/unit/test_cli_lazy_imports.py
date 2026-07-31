"""Tests for the lazy subcommand loader in `dbt_bouncer.cli.__init__`."""

import re

import pytest

import dbt_bouncer.cli

# `main.py` eagerly imports every subcommand module, so during a normal CLI run
# `__getattr__` never fires and the loader is invisible to the rest of the suite.
# Calling it directly exercises it without the sys.modules surgery that a cold
# re-import would need -- which would also build a second Typer `app` and strand
# the subcommands registered against the first.
SUBCOMMANDS = ["init", "list_checks", "run", "validate"]


@pytest.mark.parametrize("name", SUBCOMMANDS)
def test_resolves_subcommand_to_a_callable(name: str) -> None:
    """Each supported name resolves to the subcommand function of the same name."""
    resolved = dbt_bouncer.cli.__getattr__(name)

    assert callable(resolved)
    assert resolved.__name__ == name


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


def test_all_lists_every_subcommand_plus_app() -> None:
    """`__all__` stays in step with what the loader can actually resolve."""
    assert dbt_bouncer.cli.__all__ == sorted(["app", *SUBCOMMANDS])


def test_app_is_a_real_attribute_not_lazily_loaded() -> None:
    """`app` is bound at import time, so attribute access never reaches the loader."""
    assert dbt_bouncer.cli.app is not None
    assert "app" in vars(dbt_bouncer.cli)
