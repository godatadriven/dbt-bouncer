"""Tests for CheckContext."""

from typing import Any, Literal
from unittest.mock import MagicMock

import pytest
from pydantic import Field

from dbt_bouncer.check_base import BaseCheck
from dbt_bouncer.check_context import CheckContext


def test_check_context_creation():
    """CheckContext can be created with minimal arguments."""
    ctx = CheckContext(manifest_obj=None)
    assert ctx.models == []
    assert ctx.manifest_obj is None
    assert ctx.models_by_unique_id == {}


def test_check_context_with_data():
    """CheckContext stores provided data."""
    fake_models = [{"unique_id": "model.a"}]
    ctx = CheckContext(
        manifest_obj=None,
        models=fake_models,
    )
    assert ctx.models == fake_models


def test_check_context_is_frozen():
    """CheckContext is immutable."""
    ctx = CheckContext(manifest_obj=None)
    with pytest.raises(AttributeError):
        ctx.models = []  # type: ignore[misc]


class _FakeCheck(BaseCheck):
    name: Literal["fake_check"]
    model: Any = Field(default=None)

    def execute(self) -> None:
        assert self.model is not None
        assert self._ctx is not None
        assert self._ctx.manifest_obj == "test_manifest"


def test_set_resource_and_ctx():
    """Check receives resource via set_resource() and context via _ctx."""
    check = _FakeCheck(name="fake_check")

    fake_wrapper = MagicMock()
    fake_wrapper.model = "the_model"
    check.set_resource(fake_wrapper, "model")
    assert check.model == "the_model"

    ctx = CheckContext(manifest_obj="test_manifest")
    check._ctx = ctx
    check.execute()


def test_ctx_survives_model_copy():
    """_ctx is preserved through model_copy (used in runner iteration)."""
    check = _FakeCheck(name="fake_check")
    ctx = CheckContext(manifest_obj="test_manifest")
    check._ctx = ctx

    copied = check.model_copy(deep=False)
    # PrivateAttr values are preserved by model_copy
    assert copied._ctx is ctx
