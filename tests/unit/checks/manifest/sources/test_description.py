import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceDescriptionPopulated:
    def test_populated(self):
        check_passes(
            "check_source_description_populated",
            source={"description": "Description that is more than 4 characters."},
        )

    def test_multiline(self):
        check_passes(
            "check_source_description_populated",
            source={
                "description": """A
                            multiline
                            description
                            """,
            },
        )

    @pytest.mark.parametrize(
        "description",
        [
            pytest.param("", id="empty"),
            pytest.param(" ", id="whitespace"),
            pytest.param(
                """
                            """,
                id="multiline_whitespace",
            ),
            pytest.param("-", id="dash"),
            pytest.param("null", id="null_string"),
        ],
    )
    def test_not_populated(self, description):
        check_fails(
            "check_source_description_populated",
            source={"description": description},
        )
