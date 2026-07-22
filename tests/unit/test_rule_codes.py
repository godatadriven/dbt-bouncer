"""Unit tests for check rule codes."""

import re

from dbt_bouncer.check_framework.decorator import check
from dbt_bouncer.configuration_file.validator import validate_conf
from dbt_bouncer.runner import _should_run_check
from dbt_bouncer.utils import get_check_objects, get_check_registry


def test_rule_codes_assigned_and_unique() -> None:
    """All built-in checks must have a unique rule code following taxonomy."""
    checks = get_check_objects()
    assert len(checks) > 0

    codes = [getattr(c, "code", None) for c in checks]
    assert all(code is not None for code in codes), (
        "Some checks are missing rule codes."
    )

    pattern = re.compile(r"^[A-Z]{2}\d{3}$")
    invalid_codes = [c for c in codes if not pattern.match(c)]
    assert not invalid_codes, f"Invalid rule code formats found: {invalid_codes}"

    duplicate_codes = [c for c in set(codes) if codes.count(c) > 1]
    assert not duplicate_codes, f"Duplicate rule codes found: {duplicate_codes}"


def test_rule_code_registry_lookup() -> None:
    """Registry maps both check name and rule code to check class."""
    registry = get_check_registry()
    assert "check_model_access" in registry
    assert "MO001" in registry
    assert registry["check_model_access"] is registry["MO001"]


def test_rule_code_config_validation() -> None:
    """Configuration validation resolves rule codes in name/code fields."""
    raw_config = {
        "manifest_checks": [
            {"name": "MO001", "access": "public"},
            {"code": "MO005"},
        ]
    }
    conf = validate_conf(["manifest_checks"], raw_config)
    checks = conf.manifest_checks  # type: ignore[attr-defined]
    assert len(checks) == 2
    assert checks[0].name == "check_model_access"
    assert checks[0].code == "MO001"
    assert checks[1].name == "check_model_has_contracts_enforced"
    assert checks[1].code == "MO005"


def test_skip_checks_with_rule_code() -> None:
    """skip_checks in model meta supports rule codes."""

    @check(code="MO999")
    def check_fake(model):
        pass

    class DummyModelConfig:
        materialized = "table"

    class DummyModel:
        config = DummyModelConfig()

    class DummyResource:
        original_file_path = "models/my_model.sql"
        path = "models/my_model.sql"
        model = DummyModel()

    res = DummyResource()
    check_inst = check_fake()

    # Not skipped
    assert _should_run_check(check_inst, res, frozenset({"model"}), []) is True

    # Skipped by name
    assert (
        _should_run_check(check_inst, res, frozenset({"model"}), ["check_fake"])
        is False
    )

    # Skipped by rule code
    assert _should_run_check(check_inst, res, frozenset({"model"}), ["MO999"]) is False
