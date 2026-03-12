import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelDescriptionContainsRegexPattern:
    @pytest.mark.parametrize(
        ("model_override", "regexp_pattern"),
        [
            pytest.param(
                {"description": "Description that contains the pattern to match."},
                ".*pattern to match.*",
                id="contains_pattern_single_line",
            ),
            pytest.param(
                {
                    "description": "A\n                        multiline\n                        description\n                        with the pattern to match.\n                        ",
                },
                ".*pattern to match.*",
                id="contains_pattern_multiline",
            ),
            pytest.param(
                {
                    "description": "Description with\n                    the\n                    pattern to match.",
                },
                ".*pattern to match.*",
                id="contains_pattern_multiline_split",
            ),
        ],
    )
    def test_pass(self, model_override, regexp_pattern):
        check_passes(
            "check_model_description_contains_regex_pattern",
            model=model_override,
            regexp_pattern=regexp_pattern,
        )

    @pytest.mark.parametrize(
        ("model_override", "regexp_pattern"),
        [
            pytest.param(
                {"description": ""},
                ".*pattern to match.*",
                id="empty_description",
            ),
            pytest.param(
                {"description": " "},
                ".*pattern to match.*",
                id="whitespace_description",
            ),
            pytest.param(
                {"description": "\n                        "},
                ".*pattern to match.*",
                id="multiline_whitespace_description",
            ),
            pytest.param(
                {"description": "Description with a pattern that does not match."},
                ".*pattern to match.*",
                id="does_not_contain_pattern",
            ),
        ],
    )
    def test_fail(self, model_override, regexp_pattern):
        check_fails(
            "check_model_description_contains_regex_pattern",
            model=model_override,
            regexp_pattern=regexp_pattern,
        )


class TestCheckModelDescriptionPopulated:
    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"description": "Description that is more than 4 characters."},
                id="populated_description",
            ),
            pytest.param(
                {
                    "description": "A\n                        multiline\n                        description\n                        ",
                },
                id="multiline_description",
            ),
        ],
    )
    def test_pass(self, model_override):
        check_passes("check_model_description_populated", model=model_override)

    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param({"description": ""}, id="empty_description"),
            pytest.param({"description": " "}, id="whitespace_description"),
            pytest.param(
                {"description": "\n                        "},
                id="multiline_whitespace_description",
            ),
            pytest.param({"description": "-"}, id="too_short_description"),
            pytest.param({"description": "null"}, id="null_description"),
        ],
    )
    def test_fail(self, model_override):
        check_fails("check_model_description_populated", model=model_override)


class TestCheckModelDocumentationCoverage:
    @pytest.mark.parametrize(
        ("min_pct", "models_list"),
        [
            pytest.param(
                100,
                [
                    {
                        "description": "Model 2 description",
                        "name": "model_2",
                        "unique_id": "model.package_name.model_2",
                    },
                ],
                id="100_percent_coverage",
            ),
            pytest.param(
                50,
                [
                    {
                        "description": "Model 1 description",
                        "name": "model_1",
                        "unique_id": "model.package_name.model_1",
                    },
                    {
                        "description": "",
                        "name": "model_2",
                        "unique_id": "model.package_name.model_2",
                    },
                ],
                id="50_percent_coverage",
            ),
        ],
    )
    def test_pass(self, min_pct, models_list):
        check_passes(
            "check_model_documentation_coverage",
            min_model_documentation_coverage_pct=min_pct,
            ctx_models=models_list,
        )

    @pytest.mark.parametrize(
        ("min_pct", "models_list"),
        [
            pytest.param(
                100,
                [
                    {
                        "description": "",
                        "name": "model_2",
                        "unique_id": "model.package_name.model_2",
                    },
                ],
                id="0_percent_coverage",
            ),
        ],
    )
    def test_fail(self, min_pct, models_list):
        check_fails(
            "check_model_documentation_coverage",
            min_model_documentation_coverage_pct=min_pct,
            ctx_models=models_list,
        )


class TestCheckModelDocumentedInSameDirectory:
    def test_pass(self):
        check_passes(
            "check_model_documented_in_same_directory",
            model={
                "original_file_path": "models/staging/model_1.sql",
                "patch_path": "package_name://models/staging/_schema.yml",
                "path": "staging/model_1.sql",
            },
        )

    def test_fail(self):
        check_fails(
            "check_model_documented_in_same_directory",
            model={
                "original_file_path": "models/staging/finance/model_1.sql",
                "patch_path": "package_name://models/staging/_schema.yml",
                "path": "staging/finance/model_1.sql",
            },
        )
