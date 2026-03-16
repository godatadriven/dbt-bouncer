import pytest

from dbt_bouncer.testing import check_fails, check_passes


@pytest.mark.parametrize(
    ("tags", "criteria", "test_overrides", "check_fn"),
    [
        pytest.param(
            ["critical"],
            "any",
            {"tags": ["critical"]},
            check_passes,
            id="has_required_tag_any",
        ),
        pytest.param(
            ["critical", "finance"],
            "any",
            {"tags": ["critical"]},
            check_passes,
            id="has_one_of_required_tags_any",
        ),
        pytest.param(
            ["critical", "finance"],
            "all",
            {"tags": ["critical", "finance"]},
            check_passes,
            id="has_all_required_tags",
        ),
        pytest.param(
            ["critical", "finance"],
            "one",
            {"tags": ["critical"]},
            check_passes,
            id="has_exactly_one_tag",
        ),
        pytest.param(
            ["critical"],
            "any",
            {"tags": []},
            check_fails,
            id="missing_tag_any",
        ),
        pytest.param(
            ["critical"],
            "any",
            {"tags": ["finance"]},
            check_fails,
            id="has_wrong_tag_any",
        ),
        pytest.param(
            ["critical", "finance"],
            "all",
            {"tags": ["critical"]},
            check_fails,
            id="missing_one_tag_all",
        ),
        pytest.param(
            ["critical", "finance"],
            "one",
            {"tags": ["critical", "finance"]},
            check_fails,
            id="has_two_tags_when_one_expected",
        ),
    ],
)
def test_check_test_has_tags(tags, criteria, test_overrides, check_fn):
    check_fn(
        "check_test_has_tags",
        criteria=criteria,
        tags=tags,
        test=test_overrides,
    )
