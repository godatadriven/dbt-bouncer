"""Unit tests for dbt_bouncer.cli.list.utils."""

from typing import Literal

from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.cli.list.utils import category_key, get_check_params

# ---------------------------------------------------------------------------
# Helpers - lightweight stub classes to avoid importing real artifact types
# ---------------------------------------------------------------------------


def _make_class(module: str) -> type:
    """Return a bare class whose __module__ is set to *module*.

    Returns:
        type: A plain class with the given module path.

    """
    cls = type("StubCheck", (), {"__module__": module})
    return cls


# ---------------------------------------------------------------------------
# Tests for category_key
# ---------------------------------------------------------------------------


class TestCategoryKey:
    """Tests for category_key."""

    def test_manifest_module(self):
        """Module 'dbt_bouncer.checks.manifest.xxx' -> 'manifest'."""
        cls = _make_class("dbt_bouncer.checks.manifest.check_models")
        assert category_key(cls) == "manifest"

    def test_catalog_module(self):
        """Module 'dbt_bouncer.checks.catalog.xxx' -> 'catalog'."""
        cls = _make_class("dbt_bouncer.checks.catalog.check_columns")
        assert category_key(cls) == "catalog"

    def test_run_results_module(self):
        """Module 'dbt_bouncer.checks.run_results.xxx' -> 'run_results'."""
        cls = _make_class("dbt_bouncer.checks.run_results.check_run_results")
        assert category_key(cls) == "run_results"

    def test_short_module_falls_back_to_other(self):
        """A module with fewer than 3 parts returns 'other'."""
        cls = _make_class("dbt_bouncer")
        assert category_key(cls) == "other"

    def test_exactly_three_parts(self):
        """Module 'a.b.c' -> 'c' (third segment, index 2)."""
        cls = _make_class("a.b.c")
        assert category_key(cls) == "c"

    def test_two_parts_returns_other(self):
        """Module 'a.b' has length 2, which is not > 2, so returns 'other'."""
        cls = _make_class("a.b")
        assert category_key(cls) == "other"


# ---------------------------------------------------------------------------
# Tests for get_check_params
# ---------------------------------------------------------------------------


class CheckWithNoParams(BaseCheck):
    """A check that adds no extra fields."""

    name: Literal["check_with_no_params"]


class CheckWithOneParam(BaseCheck):
    """A check with a single extra parameter."""

    name: Literal["check_with_one_param"]
    my_pattern: str = Field(default=".*")


class CheckWithMultipleParams(BaseCheck):
    """A check with several extra parameters."""

    name: Literal["check_with_multiple_params"]
    threshold: int = Field(default=0)
    label: str = Field(default="")


class TestGetCheckParams:
    """Tests for get_check_params."""

    def test_no_custom_params(self):
        """A check with no extra fields returns only the 'name' discriminator.

        The 'name' field is part of every BaseCheck subclass but is NOT in
        base_fields, so get_check_params always includes it.
        """
        params = get_check_params(CheckWithNoParams)
        assert set(params.keys()) == {"name"}

    def test_single_custom_param_present(self):
        """A custom field is included in the result."""
        params = get_check_params(CheckWithOneParam)
        assert "my_pattern" in params

    def test_single_custom_param_type_string(self):
        """The type annotation for a str field is represented as 'str'."""
        params = get_check_params(CheckWithOneParam)
        assert params["my_pattern"] == "str"

    def test_multiple_custom_params(self):
        """All custom fields plus the 'name' discriminator are present in the result."""
        params = get_check_params(CheckWithMultipleParams)
        assert set(params.keys()) == {"name", "threshold", "label"}

    def test_base_fields_excluded(self):
        """Known base fields (e.g. 'model', 'severity', 'exclude') are never returned."""
        params = get_check_params(CheckWithOneParam)
        base_field_names = {
            "model",
            "severity",
            "exclude",
            "include",
            "description",
            "index",
            "manifest_obj",
        }
        assert base_field_names.isdisjoint(params.keys())

    def test_int_type_string(self):
        """An int field results in 'int' as the type string."""
        params = get_check_params(CheckWithMultipleParams)
        assert params["threshold"] == "int"
