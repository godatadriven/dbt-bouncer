from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.models.meta import (
    CheckModelHasMetaKeys,
)

_TEST_DATA_FOR_CHECK_MODEL_HAS_META_KEYS = [
    pytest.param(
        ["owner"],
        {
            "meta": {"owner": "Bob"},
        },
        does_not_raise(),
        id="has_key",
    ),
    pytest.param(
        ["owner"],
        {
            "meta": {"maturity": "high", "owner": "Bob"},
        },
        does_not_raise(),
        id="has_key_with_others",
    ),
    pytest.param(
        ["owner", {"name": ["first", "last"]}],
        {
            "meta": {
                "name": {"first": "Bob", "last": "Bobbington"},
                "owner": "Bob",
            },
        },
        does_not_raise(),
        id="has_nested_keys",
    ),
    pytest.param(
        ["key_1", "key_2"],
        {
            "meta": {"key_1": "abc", "key_2": ["a", "b", "c"]},
        },
        does_not_raise(),
        id="has_multiple_keys",
    ),
    pytest.param(
        ["owner"],
        {
            "meta": {},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_key",
    ),
    pytest.param(
        ["owner"],
        {
            "meta": {"maturity": "high"},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_key_with_others",
    ),
    pytest.param(
        ["owner", {"name": ["first", "last"]}],
        {
            "meta": {"name": {"last": "Bobbington"}, "owner": "Bob"},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_nested_key",
    ),
]


@pytest.mark.parametrize(
    ("keys", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_META_KEYS,
    indirect=["model"],
)
def test_check_model_has_meta_keys(keys, model, expectation):
    with expectation:
        CheckModelHasMetaKeys(
            keys=keys, model=model, name="check_model_has_meta_keys"
        ).execute()
