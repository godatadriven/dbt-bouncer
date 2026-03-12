from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceNames:
    def test_valid_name(self):
        check_passes(
            "check_source_names",
            source={
                "fqn": ["package_name", "source_1", "model_a"],
                "identifier": "model_a",
                "name": "model_a",
                "source_name": "source_1",
                "tags": ["tag_1"],
                "unique_id": "source.package_name.source_1.model_a",
            },
            source_name_pattern="^[a-z_]*$",
        )

    def test_invalid_name(self):
        check_fails(
            "check_source_names",
            source={
                "fqn": ["package_name", "source_1", "model_1"],
                "identifier": "model_1",
                "name": "model_1",
                "source_name": "source_1",
                "tags": ["tag_1"],
                "unique_id": "source.package_name.source_1.model_1",
            },
            source_name_pattern="^[a-z_]*$",
        )
