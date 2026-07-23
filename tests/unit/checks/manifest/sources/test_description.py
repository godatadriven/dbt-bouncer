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


class TestCheckSourceTopLevelDescriptionPopulated:
    def test_populated(self):
        check_passes(
            "check_source_top_level_description_populated",
            source={"source_description": "The CRM system of record."},
        )

    def test_min_description_length(self):
        check_passes(
            "check_source_top_level_description_populated",
            source={
                "source_description": "The CRM system of record, loaded nightly via Fivetran.",
            },
            min_description_length=25,
        )

    @pytest.mark.parametrize(
        "source_description",
        [
            pytest.param("", id="empty"),
            pytest.param(" ", id="whitespace"),
            pytest.param("-", id="dash"),
            pytest.param("null", id="null_string"),
        ],
    )
    def test_not_populated(self, source_description):
        check_fails(
            "check_source_top_level_description_populated",
            source={"source_description": source_description},
            match="top-level",
        )

    def test_shorter_than_min_description_length(self):
        check_fails(
            "check_source_top_level_description_populated",
            source={"source_description": "The CRM."},
            min_description_length=25,
            match="top-level",
        )

    def test_absent_field(self):
        # `source_description` is absent from the manifest entry entirely.
        check_fails(
            "check_source_top_level_description_populated",
            source={},
            match="top-level",
        )

    def test_table_description_does_not_satisfy_check(self):
        check_fails(
            "check_source_top_level_description_populated",
            source={"description": "A populated table-level description."},
            match="top-level",
        )
