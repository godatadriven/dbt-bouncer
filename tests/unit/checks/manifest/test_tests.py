from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import Nodes6
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.check_tests import CheckTestHasTags


@pytest.fixture
def test_node(request):
    default_test = {
        "alias": "not_null_model_1_col_1",
        "attached_node": "model.package_name.model_1",
        "checksum": {"name": "none", "checksum": ""},
        "column_name": "col_1",
        "fqn": ["package_name", "not_null_model_1_col_1"],
        "name": "not_null_model_1_col_1",
        "original_file_path": "models/_models.yml",
        "package_name": "package_name",
        "path": "not_null_model_1_col_1.sql",
        "resource_type": "test",
        "schema": "main",
        "test_metadata": {"name": "not_null"},
        "unique_id": "test.package_name.not_null_model_1_col_1.abc123",
    }
    return Nodes6(**{**default_test, **getattr(request, "param", {})})


_TEST_DATA_FOR_CHECK_TEST_HAS_TAGS = [
    pytest.param(
        ["critical"],
        "any",
        {"tags": ["critical"]},
        does_not_raise(),
        id="has_required_tag_any",
    ),
    pytest.param(
        ["critical", "finance"],
        "any",
        {"tags": ["critical"]},
        does_not_raise(),
        id="has_one_of_required_tags_any",
    ),
    pytest.param(
        ["critical", "finance"],
        "all",
        {"tags": ["critical", "finance"]},
        does_not_raise(),
        id="has_all_required_tags",
    ),
    pytest.param(
        ["critical", "finance"],
        "one",
        {"tags": ["critical"]},
        does_not_raise(),
        id="has_exactly_one_tag",
    ),
    pytest.param(
        ["critical"],
        "any",
        {"tags": []},
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_tag_any",
    ),
    pytest.param(
        ["critical"],
        "any",
        {"tags": ["finance"]},
        pytest.raises(DbtBouncerFailedCheckError),
        id="has_wrong_tag_any",
    ),
    pytest.param(
        ["critical", "finance"],
        "all",
        {"tags": ["critical"]},
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_one_tag_all",
    ),
    pytest.param(
        ["critical", "finance"],
        "one",
        {"tags": ["critical", "finance"]},
        pytest.raises(DbtBouncerFailedCheckError),
        id="has_two_tags_when_one_expected",
    ),
]


@pytest.mark.parametrize(
    ("tags", "criteria", "test_node", "expectation"),
    _TEST_DATA_FOR_CHECK_TEST_HAS_TAGS,
    indirect=["test_node"],
)
def test_check_test_has_tags(tags, criteria, test_node, expectation):
    with expectation:
        CheckTestHasTags(
            criteria=criteria,
            name="check_test_has_tags",
            tags=tags,
            test=test_node,
        ).execute()
