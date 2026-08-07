"""Tests for the public surface of `dbt_bouncer.cli`."""

import dbt_bouncer.cli
from dbt_bouncer.cli.list import list_checks


def test_all_lists_only_the_typer_app() -> None:
    """`__all__` claims only what is bound at import time -- no lazy re-exports."""
    assert dbt_bouncer.cli.__all__ == ["app"]


def test_app_is_a_real_attribute() -> None:
    """`app` is bound at import time."""
    assert dbt_bouncer.cli.app is not None
    assert "app" in vars(dbt_bouncer.cli)


def test_list_checks_is_reached_via_its_own_module() -> None:
    """`list_checks` is no longer re-exported; import it from `dbt_bouncer.cli.list`."""
    assert callable(list_checks)
    assert not hasattr(dbt_bouncer.cli, "list_checks")
