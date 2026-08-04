import re

import pytest

from dbt_bouncer.testing import check_fails, check_passes


def _model_at(original_file_path: str, **overrides) -> dict:
    """Build a model override dict from its `original_file_path`.

    `path` is derived by dropping the leading `models/` segment, matching how
    dbt populates the two fields.

    Returns:
        dict: A manifest model node dict.

    """
    path = original_file_path.removeprefix("models/")
    return {"original_file_path": original_file_path, "path": path, **overrides}


class TestCheckModelDirectories:
    @pytest.mark.parametrize(
        ("include", "model", "permitted_sub_directories", "check_fn"),
        [
            pytest.param(
                "models",
                _model_at("models/staging/stg_model_1.sql"),
                ["staging", "mart", "intermediate"],
                check_passes,
                id="valid_directory",
            ),
            pytest.param(
                "models/marts",
                _model_at("models/marts/finance/marts_model_1.sql"),
                ["finance", "marketing"],
                check_passes,
                id="valid_subdirectory",
            ),
            pytest.param(
                "models/marts/",
                _model_at("models/marts/finance/marts_model_1.sql"),
                ["finance", "marketing"],
                check_passes,
                id="valid_subdirectory_trailing_slash",
            ),
            pytest.param(
                "models/marts",
                _model_at("models/marts/sales/marts_model_1.sql"),
                ["finance", "marketing"],
                check_fails,
                id="invalid_subdirectory",
            ),
            # The model sits directly in the included directory, so the segment
            # after the match is the file itself rather than a sub-directory.
            pytest.param(
                "models",
                {
                    "original_file_path": "models/model_1.sql",
                    "path": "marts/sales/model_1.sql",
                },
                ["finance", "marketing"],
                check_fails,
                id="model_not_in_any_sub_directory",
            ),
            pytest.param(
                "^marts",
                _model_at("models/staging/stg_model_1.sql"),
                ["finance", "marketing"],
                check_fails,
                id="include_does_not_match_path",
            ),
        ],
    )
    def test_check_model_directories(
        self, include, model, permitted_sub_directories, check_fn
    ):
        check_fn(
            "check_model_directories",
            include=include,
            model=model,
            permitted_sub_directories=permitted_sub_directories,
        )

    def test_failure_message_names_the_offending_sub_directory(self):
        check_fails(
            "check_model_directories",
            include="models/marts",
            model=_model_at("models/marts/sales/marts_model_1.sql"),
            permitted_sub_directories=["finance", "marketing"],
            match=(
                r"is located in the `sales` sub-directory, this is not a valid "
                r"sub-directory \(\['finance', 'marketing'\]\)\."
            ),
        )

    def test_failure_message_when_model_is_not_in_a_sub_directory(self):
        check_fails(
            "check_model_directories",
            include="models",
            model={
                "original_file_path": "models/model_1.sql",
                "path": "marts/sales/model_1.sql",
            },
            permitted_sub_directories=["finance", "marketing"],
            match=r"is not located in a valid sub-directory",
        )


class TestCheckModelFileName:
    @pytest.mark.parametrize(
        ("file_name_pattern", "model", "check_fn"),
        [
            pytest.param(
                r".*(v[0-9])\.sql$",
                _model_at("model_v1.sql", name="model_v1"),
                check_passes,
                id="file_name_matches_pattern",
            ),
            pytest.param(
                r"  .*(v[0-9])\.sql$  ",
                _model_at("model_v1.sql", name="model_v1"),
                check_passes,
                id="padded_pattern_stripped",
            ),
            # Only the file name is matched, so the leading directories are not
            # part of the subject string.
            pytest.param(
                r"^model_v1\.sql$",
                _model_at("models/staging/model_v1.sql", name="model_v1"),
                check_passes,
                id="directories_excluded_from_match",
            ),
            pytest.param(
                ".*(v[0-9])$",
                _model_at("model_v1.sql", name="model_v1"),
                check_fails,
                id="pattern_missing_sql_suffix",
            ),
        ],
    )
    def test_check_model_file_name(self, file_name_pattern, model, check_fn):
        check_fn(
            "check_model_file_name",
            file_name_pattern=file_name_pattern,
            model=model,
        )

    def test_invalid_regex_raises_re_error(self):
        check_fails(
            "check_model_file_name",
            file_name_pattern="(unclosed",
            model=_model_at("model_v1.sql", name="model_v1"),
            expected_exception=re.error,
            match="Invalid regex pattern",
        )


class TestCheckModelHasPropertiesFile:
    @pytest.mark.parametrize(
        ("model", "check_fn"),
        [
            pytest.param(
                {
                    "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml"
                },
                check_passes,
                id="dbt_labs_convention",
            ),
            pytest.param(
                {"patch_path": "package_name://models/staging/crm/stg_model_1.yml"},
                check_passes,
                id="per_model_file",
            ),
            pytest.param({}, check_fails, id="patch_path_absent"),
            pytest.param({"patch_path": None}, check_fails, id="patch_path_none"),
            pytest.param({"patch_path": ""}, check_fails, id="patch_path_empty"),
        ],
    )
    def test_check_model_has_properties_file(self, model, check_fn):
        check_fn("check_model_has_properties_file", model=model)

    def test_failure_message(self):
        check_fails(
            "check_model_has_properties_file",
            model={},
            match="is not declared in a properties file",
        )


class TestCheckModelPropertyFileLocation:
    @pytest.mark.parametrize(
        ("model", "check_fn"),
        [
            pytest.param(
                _model_at(
                    "models/staging/crm/stg_model_1.sql",
                    patch_path="package_name://models/staging/crm/_stg_crm__models.yml",
                    unique_id="model.package_name.model_1",
                ),
                check_passes,
                id="valid_location_stg",
            ),
            pytest.param(
                _model_at(
                    "models/intermediate/crm/stg_model_1.sql",
                    patch_path="package_name://models/staging/crm/_int_crm__models.yml",
                    unique_id="model.package_name.model_1",
                ),
                check_passes,
                id="valid_location_int",
            ),
            # "marts" is dropped from the expected substring, so a marts model
            # only needs its remaining directories in the file name.
            pytest.param(
                _model_at(
                    "models/marts/crm/stg_model_1.sql",
                    patch_path="package_name://models/marts/crm/_crm__models.yml",
                    unique_id="model.package_name.model_1",
                ),
                check_passes,
                id="valid_location_marts",
            ),
            pytest.param(
                _model_at(
                    "models/staging/crm/stg_model_1.sql",
                    patch_path="package_name://models/staging/crm/_staging_crm__models.yml",
                    unique_id="model.package_name.model_1",
                ),
                check_fails,
                id="invalid_prefix",
            ),
            pytest.param(
                _model_at(
                    "models/staging/crm/stg_model_1.sql",
                    patch_path="package_name://models/staging/crm/_models.yml",
                    resource_type="model",
                    unique_id="model.package_name.model_1",
                ),
                check_fails,
                id="missing_underscore",
            ),
            pytest.param(
                _model_at(
                    "models/staging/crm/stg_model_1.sql",
                    patch_path="package_name://models/staging/crm/_schema.yml",
                    unique_id="model.package_name.model_1",
                ),
                check_fails,
                id="invalid_name",
            ),
            pytest.param(
                _model_at(
                    "models/staging/crm/stg_model_1.sql",
                    unique_id="model.package_name.model_1",
                ),
                check_fails,
                id="not_documented",
            ),
            pytest.param(
                _model_at(
                    "models/staging/crm/stg_model_1.sql",
                    patch_path="package_name://models/staging/crm/stg_crm__models.yml",
                    unique_id="model.package_name.model_1",
                ),
                check_fails,
                id="missing_leading_underscore",
            ),
            pytest.param(
                _model_at(
                    "models/staging/crm/stg_model_1.sql",
                    patch_path="package_name://models/staging/crm/_stg_crm__model.yml",
                    unique_id="model.package_name.model_1",
                ),
                check_fails,
                id="wrong_suffix",
            ),
        ],
    )
    def test_check_model_property_file_location(self, model, check_fn):
        check_fn("check_model_property_file_location", model=model)

    @pytest.mark.parametrize(
        ("patch_path", "match"),
        [
            pytest.param(
                "package_name://models/staging/crm/stg_crm__models.yml",
                r"does not start with an underscore",
                id="missing_leading_underscore",
            ),
            pytest.param(
                "package_name://models/staging/crm/_schema.yml",
                r"does not contain the expected substring \(`stg_crm`\)",
                id="missing_expected_substring",
            ),
            pytest.param(
                "package_name://models/staging/crm/_stg_crm__model.yml",
                r"does not end with `__models\.yml`",
                id="wrong_suffix",
            ),
        ],
    )
    def test_per_directory_failure_messages(self, patch_path, match):
        check_fails(
            "check_model_property_file_location",
            model=_model_at(
                "models/staging/crm/stg_model_1.sql",
                patch_path=patch_path,
                unique_id="model.package_name.model_1",
            ),
            match=match,
        )


_PER_MODEL_LAYOUT_MODEL = {
    "name": "stg_model_1",
    "original_file_path": "models/staging/crm/stg_model_1.sql",
    "path": "staging/crm/stg_model_1.sql",
    "unique_id": "model.package_name.stg_model_1",
}


class TestCheckModelPropertyFileLocationPerModelLayout:
    @pytest.mark.parametrize(
        ("patch_path", "check_fn"),
        [
            pytest.param(
                "package_name://models/staging/crm/stg_model_1.yml",
                check_passes,
                id="per_model_file",
            ),
            # Only the file name is compared, so a per-model file in another
            # directory still satisfies this check.
            pytest.param(
                "package_name://models/marts/stg_model_1.yml",
                check_passes,
                id="per_model_file_in_another_directory",
            ),
            pytest.param(
                "package_name://models/staging/crm/_stg_crm__models.yml",
                check_fails,
                id="per_directory_file",
            ),
            pytest.param(
                "package_name://models/staging/crm/other_model.yml",
                check_fails,
                id="wrong_model_name",
            ),
        ],
    )
    def test_check_model_property_file_location_per_model(self, patch_path, check_fn):
        check_fn(
            "check_model_property_file_location",
            layout="per_model",
            model={**_PER_MODEL_LAYOUT_MODEL, "patch_path": patch_path},
        )

    def test_per_model_failure_message(self):
        check_fails(
            "check_model_property_file_location",
            layout="per_model",
            model={
                **_PER_MODEL_LAYOUT_MODEL,
                "patch_path": "package_name://models/staging/crm/other_model.yml",
            },
            match=(
                r"\(`other_model\.yml`\) does not match the expected per-model file "
                r"name \(`stg_model_1\.yml`\)\."
            ),
        )

    @pytest.mark.parametrize(
        "layout",
        [
            pytest.param("per_directory", id="per_directory"),
            pytest.param("per_model", id="per_model"),
        ],
    )
    def test_undocumented_model_fails_for_every_layout(self, layout):
        # The "not documented" guard runs before the layout branch, so a model
        # with no patch_path fails identically whichever layout is configured.
        check_fails(
            "check_model_property_file_location",
            layout=layout,
            model=_PER_MODEL_LAYOUT_MODEL,
            match="is not documented",
        )

    def test_per_directory_is_the_default(self):
        # A per-model file must fail when `layout` is omitted, proving the
        # default preserves the pre-existing behaviour.
        check_fails(
            "check_model_property_file_location",
            model={
                **_PER_MODEL_LAYOUT_MODEL,
                "patch_path": "package_name://models/staging/crm/stg_model_1.yml",
            },
        )

    def test_invalid_layout_rejected(self):
        with pytest.raises(ValueError, match="per_directory"):
            check_passes(
                "check_model_property_file_location",
                layout="per_folder",
                model={},
            )


class TestCheckModelSchemaName:
    @pytest.mark.parametrize(
        ("schema_name_pattern", "model", "check_fn"),
        [
            pytest.param(
                ".*stg_.*",
                _model_at(
                    "models/staging/stg_model_1.sql",
                    name="stg_model_1",
                    schema="dbt_jdoe_stg_domain",
                    unique_id="model.package_name.stg_model_1",
                ),
                check_passes,
                id="valid_schema_stg",
            ),
            pytest.param(
                "stg_",
                _model_at(
                    "models/staging/stg_model_2.sql",
                    name="stg_model_2",
                    schema="stg_domain",
                    unique_id="model.package_name.stg_model_2",
                ),
                check_passes,
                id="valid_schema_prefixed",
            ),
            pytest.param(
                ".*_intermediate",
                _model_at(
                    "models/staging/stg_model_3.sql",
                    name="stg_model_3",
                    schema="dbt_jdoe_intermediate",
                    unique_id="model.package_name.stg_model_3",
                ),
                check_passes,
                id="valid_schema_dev_prefix",
            ),
            pytest.param(
                ".*intermediate",
                _model_at(
                    "models/intermediate/model_1.sql",
                    name="model_1",
                    schema="dbt_jdoe_int_domain",
                    unique_id="model.package_name.model_1",
                ),
                check_fails,
                id="schema_does_not_match_pattern",
            ),
            # `re.match` anchors at the start, so a bare pattern is not found
            # mid-string.
            pytest.param(
                "stg_",
                _model_at(
                    "models/staging/stg_model_1.sql",
                    name="stg_model_1",
                    schema="dbt_jdoe_stg_domain",
                    unique_id="model.package_name.stg_model_1",
                ),
                check_fails,
                id="pattern_not_anchored_at_start",
            ),
        ],
    )
    def test_check_model_schema_name(self, schema_name_pattern, model, check_fn):
        check_fn(
            "check_model_schema_name",
            schema_name_pattern=schema_name_pattern,
            model=model,
        )

    def test_failure_message_reports_the_schema_and_pattern(self):
        check_fails(
            "check_model_schema_name",
            schema_name_pattern=".*intermediate",
            model=_model_at(
                "models/intermediate/model_1.sql",
                name="model_1",
                schema="dbt_jdoe_int_domain",
                unique_id="model.package_name.model_1",
            ),
            match=r"`dbt_jdoe_int_domain` does not match the supplied regex",
        )

    def test_invalid_regex_raises_re_error(self):
        check_fails(
            "check_model_schema_name",
            schema_name_pattern="(unclosed",
            model=_model_at(
                "models/staging/stg_model_1.sql",
                name="stg_model_1",
                schema="stg_domain",
                unique_id="model.package_name.stg_model_1",
            ),
            expected_exception=re.error,
            match="Invalid regex pattern",
        )
