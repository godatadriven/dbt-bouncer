import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelNames:
    @pytest.mark.parametrize(
        ("include", "model_name_pattern", "model"),
        [
            pytest.param(
                "",
                "^stg_",
                {
                    "alias": "stg_model_1",
                    "fqn": ["package_name", "stg_model_1"],
                    "name": "stg_model_1",
                    "original_file_path": "models/staging/stg_model_1.sql",
                    "path": "staging/stg_model_1.sql",
                    "unique_id": "model.package_name.stg_model_1",
                },
                id="valid_name_stg",
            ),
            pytest.param(
                "^staging",
                "^stg_",
                {
                    "alias": "stg_model_2",
                    "fqn": ["package_name", "stg_model_2"],
                    "name": "stg_model_2",
                    "original_file_path": "models/staging/stg_model_2.sql",
                    "path": "staging/stg_model_2.sql",
                    "unique_id": "model.package_name.stg_model_2",
                },
                id="valid_name_staging_dir",
            ),
            pytest.param(
                "^intermediate",
                "^stg_",
                {
                    "alias": "stg_model_3",
                    "fqn": ["package_name", "stg_model_3"],
                    "name": "stg_model_3",
                    "original_file_path": "models/staging/stg_model_3.sql",
                    "path": "staging/stg_model_3.sql",
                    "unique_id": "model.package_name.stg_model_3",
                },
                id="valid_name_ignored_dir",
            ),
            pytest.param(
                "^intermediate",
                "^int_",
                {
                    "alias": "int_model_1",
                    "fqn": ["package_name", "int_model_1"],
                    "name": "int_model_1",
                    "original_file_path": "models/intermediate/int_model_1.sql",
                    "path": "intermediate/int_model_1.sql",
                    "unique_id": "model.package_name.int_model_1",
                },
                id="valid_name_int",
            ),
        ],
    )
    def test_passes(self, include, model_name_pattern, model):
        check_passes(
            "check_model_names",
            include=include,
            model_name_pattern=model_name_pattern,
            model=model,
        )

    @pytest.mark.parametrize(
        ("include", "model_name_pattern", "model"),
        [
            pytest.param(
                "^intermediate",
                "^int_",
                {
                    "alias": "model_1",
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "models/intermediate/model_1.sql",
                    "path": "intermediate/model_1.sql",
                    "unique_id": "model.package_name.model_1",
                },
                id="invalid_name_int",
            ),
            pytest.param(
                "^intermediate",
                "^int_",
                {
                    "alias": "model_int_2",
                    "fqn": ["package_name", "model_int_2"],
                    "name": "model_int_2",
                    "original_file_path": "models/intermediate/model_int_2.sql",
                    "path": "intermediate/model_int_2.sql",
                    "unique_id": "model.package_name.model_int_2",
                },
                id="invalid_name_int_suffix",
            ),
        ],
    )
    def test_fails(self, include, model_name_pattern, model):
        check_fails(
            "check_model_names",
            include=include,
            model_name_pattern=model_name_pattern,
            model=model,
        )
