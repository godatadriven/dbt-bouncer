"""Unit tests for dbt_bouncer.enums."""

import dbt_bouncer
from dbt_bouncer.enums import CheckCategory, Criteria, ModelAccess


class TestCheckCategory:
    """Tests for the CheckCategory enum."""

    def test_values(self):
        """Members serialise to the config-file section names."""
        assert CheckCategory.CATALOG_CHECKS == "catalog_checks"
        assert CheckCategory.MANIFEST_CHECKS == "manifest_checks"
        assert CheckCategory.RUN_RESULTS_CHECKS == "run_results_checks"

    def test_order(self):
        """Iteration order is the declaration order (alphabetical)."""
        assert [c.value for c in CheckCategory] == [
            "catalog_checks",
            "manifest_checks",
            "run_results_checks",
        ]

    def test_directory_property(self):
        """`directory` strips the '_checks' suffix to give the subdir name."""
        assert CheckCategory.CATALOG_CHECKS.directory == "catalog"
        assert CheckCategory.MANIFEST_CHECKS.directory == "manifest"
        assert CheckCategory.RUN_RESULTS_CHECKS.directory == "run_results"

    def test_directory_is_plain_str(self):
        """`directory` returns a plain ``str`` (safe for serialisation)."""
        assert type(CheckCategory.CATALOG_CHECKS.directory) is str


class TestCriteria:
    """Tests for the Criteria enum."""

    def test_values(self):
        """Members serialise to the YAML criteria values."""
        assert Criteria.ALL == "all"
        assert Criteria.ANY == "any"
        assert Criteria.ONE == "one"

    def test_members(self):
        """The enum is exactly {all, any, one}."""
        assert {c.value for c in Criteria} == {"all", "any", "one"}


class TestModelAccess:
    """Tests for the ModelAccess enum."""

    def test_values(self):
        """Members serialise to dbt's model access levels."""
        assert ModelAccess.PRIVATE == "private"
        assert ModelAccess.PROTECTED == "protected"
        assert ModelAccess.PUBLIC == "public"


class TestStrEnumBehaviour:
    """The refactor relies on StrEnum equality and string rendering."""

    def test_equality_with_plain_string(self):
        """Members compare equal to their plain string value."""
        assert ModelAccess.PUBLIC == "public"
        assert Criteria.ALL == "all"
        assert CheckCategory.MANIFEST_CHECKS == "manifest_checks"

    def test_str_and_fstring_render_the_value(self):
        """`str()`/f-strings render the value, keeping error messages stable."""
        assert str(ModelAccess.PUBLIC) == "public"
        assert f"{Criteria.ONE}" == "one"

    def test_membership_against_plain_strings(self):
        """A member is found in a collection of the equivalent plain strings."""
        assert CheckCategory.MANIFEST_CHECKS in {"manifest_checks", "catalog_checks"}
        assert "manifest_checks" in set(CheckCategory)


class TestPublicApi:
    """The new enums are re-exported from the top-level package."""

    def test_reexported(self):
        """`from dbt_bouncer import CheckCategory, Criteria, ModelAccess` works."""
        assert dbt_bouncer.CheckCategory is CheckCategory
        assert dbt_bouncer.Criteria is Criteria
        assert dbt_bouncer.ModelAccess is ModelAccess

    def test_in_dunder_all(self):
        """The new enums appear in ``__all__``."""
        assert {"CheckCategory", "Criteria", "ModelAccess"} <= set(dbt_bouncer.__all__)
