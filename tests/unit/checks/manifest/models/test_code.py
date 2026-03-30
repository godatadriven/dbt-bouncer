import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelCodeDoesNotContainRegexpPattern:
    @pytest.mark.parametrize(
        ("model_override", "regexp_pattern"),
        [
            pytest.param(
                {"raw_code": "select coalesce(a, b) from table"},
                ".*[i][f][n][u][l][l].*",
                id="does_not_contain_pattern",
            ),
        ],
    )
    def test_pass(self, model_override, regexp_pattern):
        check_passes(
            "check_model_code_does_not_contain_regexp_pattern",
            model=model_override,
            regexp_pattern=regexp_pattern,
        )

    @pytest.mark.parametrize(
        ("model_override", "regexp_pattern"),
        [
            pytest.param(
                {"raw_code": "select ifnull(a, b) from table"},
                ".*[i][f][n][u][l][l].*",
                id="contains_pattern",
            ),
        ],
    )
    def test_fail(self, model_override, regexp_pattern):
        check_fails(
            "check_model_code_does_not_contain_regexp_pattern",
            model=model_override,
            regexp_pattern=regexp_pattern,
        )


class TestCheckModelHardCodedReferences:
    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"raw_code": "SELECT * FROM {{ ref('other_model') }}", "tags": []},
                id="ref_jinja",
            ),
            pytest.param(
                {"raw_code": "SELECT * FROM {{ source('src', 'tbl') }}", "tags": []},
                id="source_jinja",
            ),
            pytest.param(
                {
                    "raw_code": "WITH cte AS (SELECT 1) SELECT * FROM cte",
                    "tags": [],
                },
                id="cte_single_part",
            ),
        ],
    )
    def test_pass(self, model_override):
        check_passes("check_model_hard_coded_references", model=model_override)

    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"raw_code": "SELECT * FROM myschema.my_table", "tags": []},
                id="bare_schema_table",
            ),
            pytest.param(
                {
                    "raw_code": "SELECT a FROM t1 JOIN myschema.other_table ON t1.id = other_table.id",
                    "tags": [],
                },
                id="bare_join_schema_table",
            ),
            pytest.param(
                {"raw_code": "SELECT * FROM catalog.schema.table_name", "tags": []},
                id="three_part_identifier",
            ),
        ],
    )
    def test_fail(self, model_override):
        check_fails("check_model_hard_coded_references", model=model_override)


class TestCheckModelHasSemiColon:
    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"raw_code": "select 1 as id", "tags": []},
                id="no_semicolon",
            ),
            pytest.param(
                {"raw_code": "select 1 as id\n                    ", "tags": []},
                id="multiline_no_semicolon",
            ),
            pytest.param(
                {
                    "raw_code": "-- comment with ;\n                    select 1 as id",
                    "tags": [],
                },
                id="semicolon_in_comment",
            ),
        ],
    )
    def test_pass(self, model_override):
        check_passes("check_model_has_semi_colon", model=model_override)

    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"raw_code": "select 1 as id;", "tags": []},
                id="semicolon",
            ),
            pytest.param(
                {"raw_code": "select 1 as id; ", "tags": []},
                id="semicolon_with_space",
            ),
            pytest.param(
                {
                    "raw_code": "select 1 as id;\n\n                    ",
                    "tags": [],
                },
                id="multiline_semicolon",
            ),
            pytest.param(
                {
                    "raw_code": "select 1 as id\n                    ; ",
                    "tags": [],
                },
                id="multiline_semicolon_next_line",
            ),
        ],
    )
    def test_fail(self, model_override):
        check_fails("check_model_has_semi_colon", model=model_override)


class TestCheckModelMaxNumberOfLines:
    def test_pass(self):
        check_passes(
            "check_model_max_number_of_lines",
            max_number_of_lines=100,
            model={
                "original_file_path": "models/staging/crm/stg_model_1.sql",
                "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
                "path": "staging/crm/stg_model_1.sql",
                "raw_code": 'with\n    source as (\n\n        {#-\n    Normally we would select from the table here, but we are using seeds to load\n    our data in this project\n    #}\n        select * from {{ ref("raw_orders") }}\n\n    ),\n\n    renamed as (\n\n        select id as order_id, user_id as customer_id, order_date, status from source\n\n    )\n\nselect *\nfrom renamed',
            },
        )

    def test_fail(self):
        check_fails(
            "check_model_max_number_of_lines",
            max_number_of_lines=10,
            model={
                "original_file_path": "models/staging/crm/stg_model_1.sql",
                "patch_path": "package_name://models/staging/crm/_schema.yml",
                "path": "staging/crm/stg_model_1.sql",
                "raw_code": 'with\n    source as (\n\n        {#-\n    Normally we would select from the table here, but we are using seeds to load\n    our data in this project\n    #}\n        select * from {{ ref("raw_orders") }}\n\n    ),\n\n    renamed as (\n\n        select id as order_id, user_id as customer_id, order_date, status from source\n\n    )\n\nselect *\nfrom renamed',
            },
        )


class TestCheckPythonModelHasSemiColon:
    def test_pass(self):
        check_passes(
            "check_model_has_semi_colon",
            model={
                "language": "python",
                "raw_code": 'def model(dbt, session):\n    df = dbt.ref("upstream")\n    return df',
                "tags": [],
            },
        )

    def test_fail(self):
        check_fails(
            "check_model_has_semi_colon",
            model={
                "language": "python",
                "raw_code": 'def model(dbt, session):\n    return dbt.ref("upstream");',
                "tags": [],
            },
        )


class TestCheckPythonModelCodeDoesNotContainRegexpPattern:
    @pytest.mark.parametrize(
        ("raw_code", "regexp_pattern"),
        [
            pytest.param(
                'import pandas as pd\n\ndef model(dbt, session):\n    df = dbt.ref("upstream")\n    return df',
                r"import\s+os",
                id="python_no_forbidden_import",
            ),
        ],
    )
    def test_pass(self, raw_code, regexp_pattern):
        check_passes(
            "check_model_code_does_not_contain_regexp_pattern",
            model={"language": "python", "raw_code": raw_code},
            regexp_pattern=regexp_pattern,
        )

    @pytest.mark.parametrize(
        ("raw_code", "regexp_pattern"),
        [
            pytest.param(
                'import os\nimport pandas as pd\n\ndef model(dbt, session):\n    df = dbt.ref("upstream")\n    return df',
                r"import\s+os",
                id="python_forbidden_import",
            ),
            pytest.param(
                'def model(dbt, session):\n    df = dbt.ref("large_table").to_pandas()\n    return df',
                r".*\.to_pandas\(\)",
                id="python_forbidden_method",
            ),
        ],
    )
    def test_fail(self, raw_code, regexp_pattern):
        check_fails(
            "check_model_code_does_not_contain_regexp_pattern",
            model={"language": "python", "raw_code": raw_code},
            regexp_pattern=regexp_pattern,
        )


class TestCheckModelMaxNumberOfLinesInvalidParam:
    @pytest.mark.parametrize(
        "max_number_of_lines",
        [
            pytest.param(0, id="zero"),
            pytest.param(-1, id="negative"),
        ],
    )
    def test_raises_value_error(self, max_number_of_lines):
        from dbt_bouncer.testing import _run_check

        with pytest.raises(ValueError, match="must be positive"):
            _run_check(
                "check_model_max_number_of_lines",
                max_number_of_lines=max_number_of_lines,
                model={"raw_code": "select 1"},
            )


class TestCheckPythonModelMaxNumberOfLines:
    def test_pass(self):
        check_passes(
            "check_model_max_number_of_lines",
            max_number_of_lines=10,
            model={
                "language": "python",
                "raw_code": 'import pandas as pd\n\ndef model(dbt, session):\n    df = dbt.ref("upstream")\n    return df',
                "tags": [],
            },
        )

    def test_fail(self):
        check_fails(
            "check_model_max_number_of_lines",
            max_number_of_lines=10,
            model={
                "language": "python",
                "raw_code": 'import pandas as pd\n# Line 2\n# Line 3\n# Line 4\n# Line 5\n# Line 6\n# Line 7\n# Line 8\n# Line 9\n# Line 10\n# Line 11\n\ndef model(dbt, session):\n    df = dbt.ref("upstream")\n    return df',
                "tags": [],
            },
        )
