import re
from types import SimpleNamespace

import pytest

from dbt_bouncer.check_framework.base import BaseCheck
from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError

# Every `_require_*` accessor and the attribute it reads. Twelve of the thirteen
# have no caller inside this repo -- they exist for class-based plugin checks,
# which `docs/CONTRIBUTING.md` documents as the supported way to reach a
# resource -- so this is the only thing guarding them against a rename.
REQUIRE_ACCESSORS = [
    ("_require_catalog_node", "catalog_node"),
    ("_require_catalog_source", "catalog_source"),
    ("_require_exposure", "exposure"),
    ("_require_macro", "macro"),
    # The only accessor whose name does not match its field.
    ("_require_manifest", "manifest_obj"),
    ("_require_model", "model"),
    ("_require_run_result", "run_result"),
    ("_require_seed", "seed"),
    ("_require_semantic_model", "semantic_model"),
    ("_require_snapshot", "snapshot"),
    ("_require_source", "source"),
    ("_require_test", "test"),
    ("_require_unit_test", "unit_test"),
]


class TestRequireAccessors:
    """Tests for the `_require_*` accessor family on BaseCheck."""

    @pytest.mark.parametrize(
        ("accessor", "field"),
        [pytest.param(a, f, id=a) for a, f in REQUIRE_ACCESSORS],
    )
    def test_returns_value_from_context(self, accessor, field):
        """Each accessor falls back to the execution context when the instance field is unset."""
        sentinel = SimpleNamespace(unique_id=f"{field}.pkg.thing")
        check = BaseCheck()
        check.set_context(SimpleNamespace(**{field: sentinel}))

        assert getattr(check, accessor)() is sentinel

    @pytest.mark.parametrize(
        ("accessor", "field"),
        [pytest.param(a, f, id=a) for a, f in REQUIRE_ACCESSORS],
    )
    def test_raises_when_unset_everywhere(self, accessor, field):
        """Each accessor raises rather than returning None when neither the instance nor the context supplies the field."""
        check = BaseCheck()

        with pytest.raises(
            DbtBouncerFailedCheckError, match=re.escape(f"self.{field} is None")
        ):
            getattr(check, accessor)()


class TestRequire:
    """Tests for BaseCheck._require."""

    def test_instance_field_takes_precedence_over_context(self):
        """A resource bound onto the instance wins over the same name on the context."""
        on_instance = SimpleNamespace(name="on_instance")
        check = BaseCheck()
        check.set_resource(on_instance, "model")
        check.set_context(SimpleNamespace(model=SimpleNamespace(name="on_context")))

        assert check._require("model") is on_instance

    def test_falls_back_to_context_when_instance_field_is_none(self):
        """An explicitly-None instance field still falls through to the context."""
        on_context = SimpleNamespace(name="on_context")
        check = BaseCheck()
        check.set_resource(None, "model")
        check.set_context(SimpleNamespace(model=on_context))

        assert check._require("model") is on_context

    def test_raises_when_context_is_unset(self):
        """With no context at all, a missing field raises instead of hitting an AttributeError."""
        check = BaseCheck()

        with pytest.raises(
            DbtBouncerFailedCheckError, match=re.escape("self.model is None")
        ):
            check._require("model")

    def test_raises_when_context_lacks_the_field(self):
        """A context that does not carry the field is treated the same as no context."""
        check = BaseCheck()
        check.set_context(SimpleNamespace(seed=SimpleNamespace(name="a_seed")))

        with pytest.raises(
            DbtBouncerFailedCheckError, match=re.escape("self.model is None")
        ):
            check._require("model")


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

        assert check._require("model") is inner

    def test_unwraps_key_from_dict(self):
        """A dict resource is unwrapped by key before binding."""
        inner = {"name": "a_model"}
        check = BaseCheck()
        check.set_resource({"model": inner}, "model")

        assert check._require("model") is inner

    def test_binds_resource_unchanged_when_not_wrapped(self):
        """An unwrapped resource is bound as-is."""
        inner = SimpleNamespace(name="a_model")
        check = BaseCheck()
        check.set_resource(inner, "model")

        assert check._require("model") is inner

    def test_dict_without_the_key_is_bound_unchanged(self):
        """A dict that does not carry the key is bound as-is rather than raising."""
        resource = {"name": "a_model"}
        check = BaseCheck()
        check.set_resource(resource, "model")

        assert check._require("model") is resource
