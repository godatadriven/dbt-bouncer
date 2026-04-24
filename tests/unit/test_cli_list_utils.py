"""Unit tests for dbt_bouncer.cli.list.utils."""

from typing import ClassVar, Literal

from pydantic import Field

from dbt_bouncer.check_framework.base import BaseCheck
from dbt_bouncer.cli.list.utils import (
    build_checks_payload,
    category_key,
    get_check_params,
    print_text_checks,
)

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


# ---------------------------------------------------------------------------
# Stub classes for build_checks_payload / print_text_checks
# ---------------------------------------------------------------------------


class _StubCheckWithDoc:
    """First line of the docstring.

    Extra detail that should be ignored.
    """

    __module__ = "dbt_bouncer.checks.manifest.check_models"
    __name__ = "StubCheckWithDoc"
    model_fields: ClassVar[dict] = {}


class _StubCheckNoDoc:
    __module__ = "dbt_bouncer.checks.manifest.check_models"
    __name__ = "StubCheckNoDoc"
    __doc__ = None
    model_fields: ClassVar[dict] = {}


class _StubCheckEmptyDoc:
    """ """  # noqa: D419

    __module__ = "dbt_bouncer.checks.catalog.check_columns"
    __name__ = "StubCheckEmptyDoc"
    model_fields: ClassVar[dict] = {}


class _StubCatalogCheck:
    """A catalog check."""

    __module__ = "dbt_bouncer.checks.catalog.check_columns"
    __name__ = "StubCatalogCheck"
    model_fields: ClassVar[dict] = {}


# ---------------------------------------------------------------------------
# Tests for build_checks_payload
# ---------------------------------------------------------------------------


class TestBuildChecksPayload:
    """Tests for build_checks_payload."""

    def test_single_check_grouped(self):
        """A single check is placed under its category label."""
        labels = {"manifest": "manifest_checks"}
        result = build_checks_payload([_StubCheckWithDoc], labels)
        assert "manifest_checks" in result
        assert len(result["manifest_checks"]) == 1

    def test_check_name_populated(self):
        """The check name matches the class __name__."""
        labels = {"manifest": "manifest_checks"}
        result = build_checks_payload([_StubCheckWithDoc], labels)
        assert result["manifest_checks"][0]["name"] == "_StubCheckWithDoc"

    def test_description_uses_first_docstring_line(self):
        """Only the first line of the docstring is used as the description."""
        labels = {"manifest": "manifest_checks"}
        result = build_checks_payload([_StubCheckWithDoc], labels)
        assert (
            result["manifest_checks"][0]["description"]
            == "First line of the docstring."
        )

    def test_no_docstring_gives_empty_description(self):
        """A check with no docstring produces an empty description."""
        labels = {"manifest": "manifest_checks"}
        result = build_checks_payload([_StubCheckNoDoc], labels)
        assert result["manifest_checks"][0]["description"] == ""

    def test_empty_docstring_gives_empty_description(self):
        """A check with a whitespace-only docstring produces an empty description."""
        labels = {"catalog": "catalog_checks"}
        result = build_checks_payload([_StubCheckEmptyDoc], labels)
        assert result["catalog_checks"][0]["description"] == ""

    def test_unknown_category_uses_raw_key(self):
        """A category not in the labels mapping falls back to the raw key."""
        result = build_checks_payload([_StubCatalogCheck], {})
        assert "catalog" in result

    def test_multiple_categories(self):
        """Checks from different categories are grouped separately."""
        labels = {"catalog": "catalog_checks", "manifest": "manifest_checks"}
        result = build_checks_payload([_StubCheckWithDoc, _StubCatalogCheck], labels)
        assert "manifest_checks" in result
        assert "catalog_checks" in result

    def test_empty_checks_list(self):
        """An empty checks list produces an empty payload."""
        result = build_checks_payload([], {})
        assert result == {}

    def test_parameters_included(self):
        """Parameters from model_fields are included in the payload."""
        labels = {"manifest": "manifest_checks"}
        result = build_checks_payload([_StubCheckWithDoc], labels)
        assert "parameters" in result["manifest_checks"][0]


# ---------------------------------------------------------------------------
# Tests for print_text_checks
# ---------------------------------------------------------------------------


class TestPrintTextChecks:
    """Tests for print_text_checks."""

    def test_category_header_printed(self, capsys):
        """The category label is printed as a header."""
        payload = {
            "manifest_checks": [
                {"name": "CheckFoo", "description": "Desc.", "parameters": {}}
            ]
        }
        print_text_checks(payload)
        output = capsys.readouterr().out
        assert "manifest_checks:" in output

    def test_check_name_printed(self, capsys):
        """The check name appears in the output."""
        payload = {
            "cat": [{"name": "CheckBar", "description": "A bar.", "parameters": {}}]
        }
        print_text_checks(payload)
        output = capsys.readouterr().out
        assert "CheckBar:" in output

    def test_description_printed(self, capsys):
        """The check description appears in the output."""
        payload = {
            "cat": [{"name": "C", "description": "My description.", "parameters": {}}]
        }
        print_text_checks(payload)
        output = capsys.readouterr().out
        assert "My description." in output

    def test_no_params_shows_none(self, capsys):
        """When a check has no parameters, '(none)' is printed."""
        payload = {"cat": [{"name": "C", "description": "D", "parameters": {}}]}
        print_text_checks(payload)
        output = capsys.readouterr().out
        assert "(none)" in output

    def test_params_printed(self, capsys):
        """Parameter names and types are printed."""
        payload = {
            "cat": [
                {"name": "C", "description": "D", "parameters": {"threshold": "int"}}
            ]
        }
        print_text_checks(payload)
        output = capsys.readouterr().out
        assert "threshold: int" in output

    def test_multiple_checks_all_printed(self, capsys):
        """All checks within a category are printed."""
        payload = {
            "cat": [
                {"name": "A", "description": "Da", "parameters": {}},
                {"name": "B", "description": "Db", "parameters": {}},
            ]
        }
        print_text_checks(payload)
        output = capsys.readouterr().out
        assert "A:" in output
        assert "B:" in output
