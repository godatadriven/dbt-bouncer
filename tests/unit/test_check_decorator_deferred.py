"""Tests for building only the requested checks (``build_only``)."""

import importlib
import inspect
import sys
import textwrap

import pytest

from dbt_bouncer.check_framework.base import BaseCheck
from dbt_bouncer.check_framework.decorator import (
    _materialise_deferred_check,
    build_only,
    materialise_deferred_checks,
)
from dbt_bouncer.utils import (
    _extract_checks_from_module,
    _get_check_module_map_cached,
    get_check_objects_for_names,
)

_MODULE_SOURCE = textwrap.dedent(
    '''
    from dbt_bouncer.check_framework.decorator import check


    @check(code="ZZ001")
    def check_model_alpha(model):
        """Alpha."""


    @check
    def check_model_beta(model, *, threshold: int = 1):
        """Beta."""


    @check(code="ZZ003")
    def check_model_gamma(model):
        """Gamma."""
    '''
)


@pytest.fixture
def import_check_module(tmp_path, monkeypatch):
    """Import a fresh three-check module, optionally inside ``build_only``.

    Yields:
        Callable: Takes the requested names (or ``None``) and returns the module.

    """
    module_name = "deferred_checks_under_test"
    (tmp_path / f"{module_name}.py").write_text(_MODULE_SOURCE)
    monkeypatch.syspath_prepend(str(tmp_path))
    monkeypatch.delitem(sys.modules, module_name, raising=False)

    def _import(requested: frozenset[str] | None):
        if requested is None:
            return importlib.import_module(module_name)
        with build_only(requested):
            return importlib.import_module(module_name)

    yield _import
    sys.modules.pop(module_name, None)


def test_build_only_builds_requested_names_and_codes(import_check_module):
    module = import_check_module(frozenset({"check_model_alpha", "ZZ003"}))

    assert issubclass(module.__dict__["CheckModelAlpha"], BaseCheck)
    assert issubclass(module.__dict__["CheckModelGamma"], BaseCheck)
    assert "CheckModelBeta" not in module.__dict__
    # Until it is built, the function stands in under its own name.
    assert inspect.isfunction(module.__dict__["check_model_beta"])


def test_without_build_only_every_check_is_built(import_check_module):
    module = import_check_module(None)

    assert {"CheckModelAlpha", "CheckModelBeta", "CheckModelGamma"} <= set(
        module.__dict__
    )
    assert "__getattr__" not in module.__dict__


def test_attribute_access_builds_a_deferred_check(import_check_module):
    module = import_check_module(frozenset({"check_model_alpha"}))

    beta = module.CheckModelBeta

    assert issubclass(beta, BaseCheck)
    assert beta.model_fields["threshold"].default == 1
    # The class replaces the stand-in function, as an eager `@check` does.
    assert module.check_model_beta is beta
    assert module.CheckModelBeta is beta


def test_unknown_attribute_still_raises(import_check_module):
    module = import_check_module(frozenset({"check_model_alpha"}))

    with pytest.raises(AttributeError, match="has no attribute 'CheckModelDelta'"):
        _ = module.CheckModelDelta
    assert not hasattr(module, "CheckModelDelta")


def test_materialise_builds_a_subset_then_the_rest(import_check_module):
    module = import_check_module(frozenset({"check_model_alpha"}))

    materialise_deferred_checks(module, frozenset({"ZZ003"}))
    assert "CheckModelGamma" in module.__dict__
    assert "CheckModelBeta" not in module.__dict__

    materialise_deferred_checks(module)
    assert "CheckModelBeta" in module.__dict__


def test_materialise_is_a_no_op_without_deferred_checks(import_check_module):
    module = import_check_module(None)

    materialise_deferred_checks(module)

    assert _materialise_deferred_check(module, "CheckModelAlpha") is (
        module.CheckModelAlpha
    )
    # A name that is neither deferred nor a class is not a check.
    assert _materialise_deferred_check(module, "check") is None


def test_extracting_checks_builds_the_deferred_ones(import_check_module):
    module = import_check_module(frozenset({"check_model_alpha"}))
    found: list[type[BaseCheck]] = []

    _extract_checks_from_module(module, module.__name__, found)

    assert sorted(cls.__name__ for cls in found) == [
        "CheckModelAlpha",
        "CheckModelBeta",
        "CheckModelGamma",
    ]


def test_get_check_objects_for_names_builds_only_requested(monkeypatch):
    """A fresh import of a real check module builds only the requested check."""
    module_name = "dbt_bouncer.checks.manifest.check_seeds"
    package = importlib.import_module("dbt_bouncer.checks.manifest")
    original = importlib.import_module(module_name)
    # Build the name-to-module map now: building it imports every module eagerly.
    _get_check_module_map_cached(None)
    # Restore the original module (and the package attribute) after the test.
    monkeypatch.delitem(sys.modules, module_name)
    monkeypatch.setattr(package, "check_seeds", original)

    loaded = get_check_objects_for_names(frozenset({"check_seed_names"}))

    fresh = sys.modules[module_name]
    assert fresh is not original
    assert [cls.__name__ for cls in loaded if cls.__module__ == module_name] == [
        "CheckSeedNames"
    ]
    # The other seed checks are still reachable on demand.
    assert issubclass(fresh.CheckSeedDescriptionPopulated, BaseCheck)


def test_get_check_objects_for_names_builds_checks_deferred_earlier(monkeypatch):
    """A module that an earlier call imported with other names still yields the request."""
    module_name = "dbt_bouncer.checks.manifest.check_seeds"
    package = importlib.import_module("dbt_bouncer.checks.manifest")
    monkeypatch.setattr(package, "check_seeds", importlib.import_module(module_name))
    _get_check_module_map_cached(None)
    monkeypatch.delitem(sys.modules, module_name)

    get_check_objects_for_names(frozenset({"check_seed_names"}))
    loaded = get_check_objects_for_names(
        frozenset({"check_seed_description_populated"})
    )

    assert "CheckSeedDescriptionPopulated" in [cls.__name__ for cls in loaded]
