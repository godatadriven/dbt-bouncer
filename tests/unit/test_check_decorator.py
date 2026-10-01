"""Tests for the @check decorator."""

import pytest
from pydantic import ValidationError

from dbt_bouncer.check_framework.base import BaseCheck
from dbt_bouncer.check_framework.decorator import check, fail
from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError

# --- Decorator-defined checks for testing ---


@check
def check_decorator_basic(model):
    """Validate basic decorator check."""
    if not model.name:
        fail("Model has no name.")


CheckDecoratorBasic = check_decorator_basic


@check
def check_decorator_with_params(model, *, min_length: int):
    """Validate check with user-configurable params."""
    if len(model.name) < min_length:
        fail(f"Model name `{model.name}` is shorter than {min_length}.")


CheckDecoratorWithParams = check_decorator_with_params


@check
def check_decorator_with_default_param(model, *, threshold: int = 10):
    """Validate check with a param that has a default value."""


CheckDecoratorWithDefaultParam = check_decorator_with_default_param


@check
def check_decorator_context_only(ctx):
    """Validate context-only check (no iterate_over)."""
    if not ctx.models:
        fail("No models found.")


CheckDecoratorContextOnly = check_decorator_context_only


@check
def check_decorator_no_ctx(model):
    """Validate check that doesn't need ctx."""
    if not model.name:
        fail("Model has no name.")


CheckDecoratorNoCtx = check_decorator_no_ctx


@check
def check_decorator_resource_and_ctx(model, ctx):
    """Validate check that uses both resource and ctx."""
    if not ctx.models:
        fail("No models in context.")
    if not model.name:
        fail("Model has no name.")


CheckDecoratorResourceAndCtx = check_decorator_resource_and_ctx


def _min_not_above_max(*, max_count: int, min_count: int) -> None:
    if min_count > max_count:
        raise ValueError("`min_count` must not exceed `max_count`.")


@check(validate=_min_not_above_max)
def check_decorator_with_validate(model, *, max_count: int = 10, min_count: int = 0):
    """Validate a check with a cross-parameter validator."""


CheckDecoratorWithValidate = check_decorator_with_validate


# --- Tests ---


class TestCheckDecoratorClassGeneration:
    def test_generates_base_check_subclass(self):
        assert issubclass(CheckDecoratorBasic, BaseCheck)

    def test_class_name_is_pascal_case(self):
        assert CheckDecoratorBasic.__name__ == "CheckDecoratorBasic"

    def test_preserves_docstring(self):
        assert CheckDecoratorBasic.__doc__ == "Validate basic decorator check."

    def test_name_field_has_correct_literal(self):
        instance = CheckDecoratorBasic(name="check_decorator_basic")
        assert instance.name == "check_decorator_basic"

    def test_iterate_over_classvar_is_set(self):
        assert CheckDecoratorBasic.iterate_over == "model"

    def test_context_only_has_no_iterate_over(self):
        assert CheckDecoratorContextOnly.iterate_over is None

    def test_resource_field_exists(self):
        assert "model" in CheckDecoratorBasic.model_fields

    def test_param_fields_exist(self):
        assert "min_length" in CheckDecoratorWithParams.model_fields

    def test_param_with_default_has_correct_default(self):
        instance = CheckDecoratorWithDefaultParam(
            name="check_decorator_with_default_param",
        )
        assert instance.threshold == 10


class TestCheckDecoratorExecution:
    def test_passes_when_condition_met(self):
        from dbt_bouncer.artifact_parsers.parser import wrap_dict

        model = wrap_dict({"name": "stg_orders", "unique_id": "model.pkg.stg_orders"})
        instance = CheckDecoratorBasic(name="check_decorator_basic", model=model)
        instance.execute()

    def test_fails_when_condition_not_met(self):
        from dbt_bouncer.artifact_parsers.parser import wrap_dict

        model = wrap_dict({"name": "", "unique_id": "model.pkg.empty"})
        instance = CheckDecoratorBasic(name="check_decorator_basic", model=model)
        with pytest.raises(DbtBouncerFailedCheckError, match="Model has no name"):
            instance.execute()

    def test_params_are_passed(self):
        from dbt_bouncer.artifact_parsers.parser import wrap_dict

        model = wrap_dict({"name": "ab", "unique_id": "model.pkg.ab"})
        instance = CheckDecoratorWithParams(
            name="check_decorator_with_params",
            model=model,
            min_length=5,
        )
        with pytest.raises(DbtBouncerFailedCheckError, match="shorter than 5"):
            instance.execute()

    def test_context_only_check_with_models(self):
        from dbt_bouncer.artifact_parsers.parser import wrap_dict
        from dbt_bouncer.check_framework.context import CheckContext

        ctx = CheckContext(models=[wrap_dict({"name": "m1"})])
        instance = CheckDecoratorContextOnly(name="check_decorator_context_only")
        instance.set_context(ctx)
        instance.execute()

    def test_context_only_check_without_models(self):
        from dbt_bouncer.check_framework.context import CheckContext

        ctx = CheckContext(models=[])
        instance = CheckDecoratorContextOnly(name="check_decorator_context_only")
        instance.set_context(ctx)
        with pytest.raises(DbtBouncerFailedCheckError, match="No models found"):
            instance.execute()

    def test_no_ctx_check_passes(self):
        from dbt_bouncer.artifact_parsers.parser import wrap_dict

        model = wrap_dict({"name": "stg_orders", "unique_id": "model.pkg.stg_orders"})
        instance = CheckDecoratorNoCtx(name="check_decorator_no_ctx", model=model)
        instance.execute()

    def test_resource_and_ctx_check_passes(self):
        from dbt_bouncer.artifact_parsers.parser import wrap_dict
        from dbt_bouncer.check_framework.context import CheckContext

        model = wrap_dict({"name": "stg_orders", "unique_id": "model.pkg.stg_orders"})
        ctx = CheckContext(models=[model])
        instance = CheckDecoratorResourceAndCtx(
            name="check_decorator_resource_and_ctx", model=model
        )
        instance.set_context(ctx)
        instance.execute()


class TestFailHelper:
    def test_raises_failed_check_error(self):
        with pytest.raises(DbtBouncerFailedCheckError, match="test message"):
            fail("test message")


class TestCheckDecoratorValidate:
    """`@check(validate=...)` rejects bad parameters when the check is built."""

    def test_valid_params_construct(self):
        instance = CheckDecoratorWithValidate(
            name="check_decorator_with_validate", max_count=5, min_count=5
        )
        assert instance.min_count == 5

    def test_invalid_params_raise_validation_error_at_construction(self):
        # Construction is what config loading does, so the error surfaces as a
        # config error before any check runs, not once per resource.
        with pytest.raises(ValidationError, match="must not exceed `max_count`"):
            CheckDecoratorWithValidate(
                name="check_decorator_with_validate", max_count=1, min_count=2
            )

    def test_defaults_are_validated(self):
        # The validator sees every keyword-only param, including defaulted ones.
        with pytest.raises(ValidationError, match="must not exceed"):
            CheckDecoratorWithValidate(
                name="check_decorator_with_validate", max_count=-1
            )

    def test_check_without_validate_has_no_extra_validator(self):
        instance = CheckDecoratorWithParams(
            name="check_decorator_with_params", min_length=-100
        )
        assert instance.min_length == -100
