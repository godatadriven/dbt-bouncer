import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourceNames:
    @pytest.mark.parametrize(
        ("source", "source_name_pattern", "check_fn"),
        [
            pytest.param(
                {
                    "fqn": ["package_name", "source_1", "model_a"],
                    "identifier": "model_a",
                    "name": "model_a",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.model_a",
                },
                "^[a-z_]*$",
                check_passes,
                id="valid_name",
            ),
            pytest.param(
                {
                    "fqn": ["package_name", "source_1", "model_1"],
                    "identifier": "model_1",
                    "name": "model_1",
                    "source_name": "source_1",
                    "tags": ["tag_1"],
                    "unique_id": "source.package_name.source_1.model_1",
                },
                "^[a-z_]*$",
                check_fails,
                id="invalid_name",
            ),
        ],
    )
    def test_check_source_names(self, source, source_name_pattern, check_fn):
        check_fn(
            "check_source_names",
            source=source,
            source_name_pattern=source_name_pattern,
        )
