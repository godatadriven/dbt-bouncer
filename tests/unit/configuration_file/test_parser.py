"""Unit tests for dbt_bouncer.configuration_file.parser."""

from typing import Any

from dbt_bouncer.configuration_file.parser import DbtBouncerConfBase, get_check_types


def test_get_check_types_returns_list_any_when_no_checks_match():
    """A category with no matching check classes falls back to ``list[Any]``."""
    assert get_check_types("manifest", check_objects=[]) == list[Any]


def test_dbt_bouncer_conf_base_eq_returns_not_implemented_for_other_types():
    """Comparing against a non-config object defers to Python's default equality."""
    conf = DbtBouncerConfBase()

    assert conf.__eq__(object()) is NotImplemented
    assert conf != "not a config"


def test_dbt_bouncer_conf_base_eq_compares_field_values():
    """Two configs with identical field values compare equal."""
    assert DbtBouncerConfBase(severity="warn") == DbtBouncerConfBase(severity="warn")
    assert DbtBouncerConfBase(severity="warn") != DbtBouncerConfBase(severity="error")
