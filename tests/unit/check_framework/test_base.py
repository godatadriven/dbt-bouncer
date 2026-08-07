from types import SimpleNamespace

import pytest

from dbt_bouncer.check_framework.base import BaseCheck


class TestIsDescriptionPopulated:
    """Tests for BaseCheck._is_description_populated."""

    @pytest.mark.parametrize(
        ("description", "expected"),
        [
            pytest.param("A real description.", True, id="populated"),
            pytest.param("", False, id="empty"),
            pytest.param("   ", False, id="whitespace_only"),
            pytest.param("abc", False, id="shorter_than_default_minimum"),
            pytest.param("abcd", True, id="exactly_default_minimum"),
            pytest.param("N/A", False, id="placeholder_na"),
            pytest.param("none", False, id="placeholder_none"),
            pytest.param("null", False, id="placeholder_null"),
        ],
    )
    def test_uses_class_default_minimum_length(self, description, expected):
        """Passing None for the minimum length falls back to the class-level default of 4."""
        assert BaseCheck()._is_description_populated(description, None) is expected

    @pytest.mark.parametrize(
        ("description", "min_description_length", "expected"),
        [
            pytest.param("abcdefghij", 10, True, id="exactly_override"),
            pytest.param("abcdefghi", 10, False, id="shorter_than_override"),
            pytest.param("abc", 2, True, id="override_below_class_default"),
        ],
    )
    def test_explicit_minimum_length_overrides_the_default(
        self, description, min_description_length, expected
    ):
        """An explicit minimum length is used in place of the class-level default."""
        check = BaseCheck()

        assert (
            check._is_description_populated(description, min_description_length)
            is expected
        )

    def test_zero_minimum_length_falls_back_to_the_default(self):
        """A minimum length of 0 is falsy, so the class default applies -- 0 cannot be used to disable the check."""
        assert BaseCheck()._is_description_populated("abc", 0) is False


class TestSetResource:
    """Tests for BaseCheck.set_resource."""

    def test_unwraps_attribute_from_wrapper_object(self):
        """A wrapper exposing the resource as an attribute is unwrapped before binding."""
        inner = SimpleNamespace(name="a_model")
        check = BaseCheck()
        check.set_resource(SimpleNamespace(model=inner), "model")

        assert check.model is inner

    def test_unwraps_key_from_dict(self):
        """A dict resource is unwrapped by key before binding."""
        inner = {"name": "a_model"}
        check = BaseCheck()
        check.set_resource({"model": inner}, "model")

        assert check.model is inner

    def test_binds_resource_unchanged_when_not_wrapped(self):
        """An unwrapped resource is bound as-is."""
        inner = SimpleNamespace(name="a_model")
        check = BaseCheck()
        check.set_resource(inner, "model")

        assert check.model is inner

    def test_dict_without_the_key_is_bound_unchanged(self):
        """A dict that does not carry the key is bound as-is rather than raising."""
        resource = {"name": "a_model"}
        check = BaseCheck()
        check.set_resource(resource, "model")

        assert check.model is resource
