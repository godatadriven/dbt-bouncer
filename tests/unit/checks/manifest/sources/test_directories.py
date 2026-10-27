import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckSourcePropertyFileLocation:
    def test_correct_location(self):
        check_passes(
            "check_source_property_file_location",
            source={
                "original_file_path": "models/staging/crm/_crm__sources.yml",
                "path": "models/staging/crm/_crm__sources.yml",
                "tags": ["tag_1"],
            },
        )

    def test_correct_location_staging_root(self):
        check_passes(
            "check_source_property_file_location",
            source={
                "original_file_path": "models/staging/_staging__sources.yml",
                "path": "models/staging/_staging__sources.yml",
                "tags": ["tag_1"],
            },
        )

    def test_correct_location_custom_model_path(self):
        check_passes(
            "check_source_property_file_location",
            source={
                "original_file_path": "transformations/staging/crm/_crm__sources.yml",
                "path": "transformations/staging/crm/_crm__sources.yml",
                "tags": ["tag_1"],
            },
        )

    @pytest.mark.parametrize(
        "original_file_path",
        [
            pytest.param("models/_sources.yml", id="models_root"),
            pytest.param("models/marts/_models_marts__sources.yml", id="marts"),
            pytest.param(
                "models/marts/my_model/_my_model__sources.yml", id="marts_subdir"
            ),
            pytest.param(
                "transformations/marts/_marts__sources.yml", id="custom_model_path"
            ),
        ],
    )
    def test_outside_staging(self, original_file_path):
        check_fails(
            "check_source_property_file_location",
            source={
                "original_file_path": original_file_path,
                "path": original_file_path,
                "tags": ["tag_1"],
            },
            match=r"is not located in `\w+/staging/`",
        )

    @pytest.mark.parametrize(
        ("original_file_path", "path"),
        [
            pytest.param(
                "models/staging/crm/_crm__source.yml",
                "models/staging/crm/_crm__source.yml",
                id="singular_source",
            ),
            pytest.param(
                "models/staging/crm/__source.yml",
                "models/staging/crm/__source.yml",
                id="double_underscore",
            ),
            pytest.param(
                "models/staging/crm/_staging__source.yml",
                "models/staging/crm/_staging__source.yml",
                id="wrong_prefix",
            ),
            pytest.param(
                "models/staging/crm/crm__source.yml",
                "models/staging/crm/crm__source.yml",
                id="no_leading_underscore",
            ),
        ],
    )
    def test_incorrect_location(self, original_file_path, path):
        check_fails(
            "check_source_property_file_location",
            source={
                "original_file_path": original_file_path,
                "path": path,
                "tags": ["tag_1"],
            },
        )


class TestCheckSourceFileName:
    @pytest.mark.parametrize(
        ("original_file_path", "file_name_pattern"),
        [
            pytest.param(
                "models/staging/crm/_crm__sources.yml",
                "^.*__sources\\.yml$",
                id="simple_match",
            ),
            pytest.param(
                "models/staging/crm/_crm__sources.yml",
                "^_.*\\.yml$",
                id="leading_underscore",
            ),
        ],
    )
    def test_pass(self, original_file_path, file_name_pattern):
        check_passes(
            "check_source_file_name",
            source={
                "name": "my_source",
                "original_file_path": original_file_path,
                "source_name": "crm",
            },
            file_name_pattern=file_name_pattern,
        )

    @pytest.mark.parametrize(
        ("original_file_path", "file_name_pattern"),
        [
            pytest.param(
                "models/staging/crm/crm.yml",
                "^.*__sources\\.yml$",
                id="missing_suffix",
            ),
            pytest.param(
                "models/staging/crm/_crm__sources.yaml",
                "^.*__sources\\.yml$",
                id="wrong_extension",
            ),
            pytest.param(
                "models/staging/crm/_crm__sourcesXyml",
                "^.*__sources\\.yml$",
                id="unescaped_dot_check",
            ),
        ],
    )
    def test_fail(self, original_file_path, file_name_pattern):
        check_fails(
            "check_source_file_name",
            source={
                "name": "my_source",
                "original_file_path": original_file_path,
                "source_name": "crm",
            },
            file_name_pattern=file_name_pattern,
        )
