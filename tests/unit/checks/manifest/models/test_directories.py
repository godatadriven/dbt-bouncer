import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelDirectories:
    @pytest.mark.parametrize(
        ("include", "model", "permitted_sub_directories"),
        [
            pytest.param(
                "models",
                {
                    "original_file_path": "models/staging/stg_model_1.sql",
                    "path": "staging/stg_model_1.sql",
                },
                ["staging", "mart", "intermediate"],
                id="valid_directory",
            ),
            pytest.param(
                "models/marts",
                {
                    "original_file_path": "models/marts/finance/marts_model_1.sql",
                    "path": "marts/finance/marts_model_1.sql",
                },
                ["finance", "marketing"],
                id="valid_subdirectory",
            ),
            pytest.param(
                "models/marts/",
                {
                    "original_file_path": "models/marts/finance/marts_model_1.sql",
                    "path": "marts/finance/marts_model_1.sql",
                },
                ["finance", "marketing"],
                id="valid_subdirectory_trailing_slash",
            ),
        ],
    )
    def test_passes(self, include, model, permitted_sub_directories):
        check_passes(
            "check_model_directories",
            include=include,
            model=model,
            permitted_sub_directories=permitted_sub_directories,
        )

    @pytest.mark.parametrize(
        ("include", "model", "permitted_sub_directories"),
        [
            pytest.param(
                "models/marts",
                {
                    "original_file_path": "models/marts/sales/marts_model_1.sql",
                    "path": "marts/sales/marts_model_1.sql",
                },
                ["finance", "marketing"],
                id="invalid_subdirectory",
            ),
            pytest.param(
                "models",
                {
                    "original_file_path": "models/model_1.sql",
                    "path": "marts/sales/model_1.sql",
                },
                ["finance", "marketing"],
                id="invalid_root_directory",
            ),
        ],
    )
    def test_fails(self, include, model, permitted_sub_directories):
        check_fails(
            "check_model_directories",
            include=include,
            model=model,
            permitted_sub_directories=permitted_sub_directories,
        )


class TestCheckModelFileName:
    def test_passes(self):
        check_passes(
            "check_model_file_name",
            file_name_pattern=r".*(v[0-9])\.sql$",
            model={
                "alias": "model_v1",
                "config": {"grants": {"select": ["user1"]}},
                "fqn": ["package_name", "model_v1"],
                "name": "model_v1",
                "original_file_path": "model_v1.sql",
                "path": "staging/finance/model_v1.sql",
                "unique_id": "model.package_name.model_v1",
            },
        )

    def test_fails(self):
        check_fails(
            "check_model_file_name",
            file_name_pattern=".*(v[0-9])$",
            model={
                "alias": "model_v1",
                "config": {"grants": {"select": ["user1"]}},
                "fqn": ["package_name", "model_v1"],
                "name": "model_v1",
                "original_file_path": "model_v1.sql",
                "path": "staging/finance/model_v1.sql",
                "unique_id": "model.package_name.model_v1",
            },
        )


class TestCheckModelPropertyFileLocation:
    @pytest.mark.parametrize(
        "model",
        [
            pytest.param(
                {
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "unique_id": "model.package_name.model_1",
                },
                id="valid_location_stg",
            ),
            pytest.param(
                {
                    "original_file_path": "models/intermediate/crm/stg_model_1.sql",
                    "patch_path": "package_name://models/staging/crm/_int_crm__models.yml",
                    "path": "intermediate/crm/stg_model_1.sql",
                    "unique_id": "model.package_name.model_1",
                },
                id="valid_location_int",
            ),
            pytest.param(
                {
                    "original_file_path": "models/marts/crm/stg_model_1.sql",
                    "patch_path": "package_name://models/marts/crm/_crm__models.yml",
                    "path": "marts/crm/stg_model_1.sql",
                    "unique_id": "model.package_name.model_1",
                },
                id="valid_location_marts",
            ),
        ],
    )
    def test_passes(self, model):
        check_passes("check_model_property_file_location", model=model)

    @pytest.mark.parametrize(
        "model",
        [
            pytest.param(
                {
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "patch_path": "package_name://models/staging/crm/_staging_crm__models.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "unique_id": "model.package_name.model_1",
                },
                id="invalid_prefix",
            ),
            pytest.param(
                {
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "patch_path": "package_name://models/staging/crm/_models.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "resource_type": "model",
                    "unique_id": "model.package_name.model_1",
                },
                id="missing_underscore",
            ),
            pytest.param(
                {
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "patch_path": "package_name://models/staging/crm/_schema.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "unique_id": "model.package_name.model_1",
                },
                id="invalid_name",
            ),
        ],
    )
    def test_fails(self, model):
        check_fails("check_model_property_file_location", model=model)


class TestCheckModelSchemaName:
    @pytest.mark.parametrize(
        ("include", "schema_name_pattern", "model"),
        [
            pytest.param(
                "",
                ".*stg_.*",
                {
                    "alias": "stg_model_1",
                    "fqn": ["package_name", "stg_model_1"],
                    "name": "stg_model_1",
                    "original_file_path": "models/staging/stg_model_1.sql",
                    "path": "staging/stg_model_1.sql",
                    "schema": "dbt_jdoe_stg_domain",
                    "unique_id": "model.package_name.stg_model_1",
                },
                id="valid_schema_stg",
            ),
            pytest.param(
                "^staging",
                "stg_",
                {
                    "alias": "stg_model_2",
                    "fqn": ["package_name", "stg_model_2"],
                    "name": "stg_model_2",
                    "original_file_path": "models/staging/stg_model_2.sql",
                    "path": "staging/stg_model_2.sql",
                    "schema": "stg_domain",
                    "unique_id": "model.package_name.stg_model_2",
                },
                id="valid_schema_staging_dir",
            ),
            pytest.param(
                "^intermediate",
                ".*_intermediate",
                {
                    "alias": "stg_model_3",
                    "fqn": ["package_name", "stg_model_3"],
                    "name": "stg_model_3",
                    "original_file_path": "models/staging/stg_model_3.sql",
                    "path": "staging/stg_model_3.sql",
                    "schema": "dbt_jdoe_intermediate",
                    "unique_id": "model.package_name.stg_model_3",
                },
                id="valid_schema_ignored_dir",
            ),
        ],
    )
    def test_passes(self, include, schema_name_pattern, model):
        check_passes(
            "check_model_schema_name",
            include=include,
            schema_name_pattern=schema_name_pattern,
            model=model,
        )

    def test_fails(self):
        check_fails(
            "check_model_schema_name",
            include="^intermediate",
            schema_name_pattern=".*intermediate",
            model={
                "alias": "model_1",
                "fqn": ["package_name", "model_1"],
                "name": "model_1",
                "original_file_path": "models/intermediate/model_1.sql",
                "path": "intermediate/model_1.sql",
                "schema": "dbt_jdoe_int_domain",
                "unique_id": "model.package_name.model_1",
            },
        )
