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


class TestCheckModelDoesNotUseSelectStar:
    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"raw_code": "SELECT id, name FROM my_table"},
                id="explicit_columns",
            ),
            pytest.param(
                {"raw_code": ""},
                id="empty_raw_code",
            ),
            pytest.param(
                {"raw_code": None},
                id="none_raw_code",
            ),
            pytest.param(
                {"raw_code": "-- SELECT * FROM old_table\nSELECT id FROM my_table"},
                id="select_star_only_in_line_comment",
            ),
            pytest.param(
                {"raw_code": "{# SELECT * FROM old_table #}\nSELECT id FROM my_table"},
                id="select_star_only_in_jinja_comment",
            ),
            pytest.param(
                {"raw_code": "/* SELECT * FROM old_table */\nSELECT id FROM my_table"},
                id="select_star_only_in_block_comment",
            ),
            pytest.param(
                {"raw_code": "SELECT count(*) FROM my_table"},
                id="count_star_is_not_select_star",
            ),
            pytest.param(
                {"raw_code": "SELECT 'select * from x' AS note FROM my_table"},
                id="select_star_only_in_string_literal",
            ),
        ],
    )
    def test_pass(self, model_override):
        check_passes("check_model_does_not_use_select_star", model=model_override)

    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"raw_code": "select * from my_table"},
                id="lowercase_select_star",
            ),
            pytest.param(
                {"raw_code": "SELECT * FROM my_table"},
                id="uppercase_select_star",
            ),
            pytest.param(
                {"raw_code": "SELECT  * FROM my_table"},
                id="select_star_extra_space",
            ),
            pytest.param(
                {
                    "raw_code": "WITH cte AS (SELECT id FROM t) SELECT * FROM cte",
                },
                id="select_star_in_outer_query",
            ),
            pytest.param(
                {"raw_code": "SELECT DISTINCT * FROM my_table"},
                id="select_distinct_star",
            ),
            pytest.param(
                {"raw_code": "SELECT ALL * FROM my_table"},
                id="select_all_star",
            ),
            pytest.param(
                {"raw_code": "SELECT t.* FROM my_table AS t"},
                id="qualified_star",
            ),
            pytest.param(
                {
                    "raw_code": "{% if true %}SELECT * FROM {{ ref('m') }}{% else %}SELECT * FROM {{ ref('n') }}{% endif %}",
                },
                id="jinja_control_flow_falls_back_and_fails",
            ),
        ],
    )
    def test_fail(self, model_override):
        check_fails("check_model_does_not_use_select_star", model=model_override)


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
            pytest.param(
                {
                    "raw_code": "{% if true %}SELECT a FROM {{ ref('m') }}{% else %}SELECT b FROM {{ ref('n') }}{% endif %}",
                    "tags": [],
                },
                id="jinja_control_flow_falls_back_and_passes",
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
            pytest.param(
                {"raw_code": 'SELECT * FROM "myschema"."my_table"', "tags": []},
                id="quoted_schema_table",
            ),
        ],
    )
    def test_fail(self, model_override):
        check_fails("check_model_hard_coded_references", model=model_override)


class TestCheckPythonModelDoesNotUseSelectStar:
    def test_pass(self):
        # Python models are not SQL, so the check skips them even when their
        # source contains a literal "SELECT *".
        check_passes(
            "check_model_does_not_use_select_star",
            model={
                "language": "python",
                "raw_code": 'def model(dbt, session):\n    return dbt.ref("upstream")  # SELECT * FROM schema.table',
                "tags": [],
            },
        )


class TestCheckPythonModelHardCodedReferences:
    def test_pass(self):
        # Python models are not SQL, so the check skips them even when their
        # source contains a hard-coded dotted reference.
        check_passes(
            "check_model_hard_coded_references",
            model={
                "language": "python",
                "raw_code": 'def model(dbt, session):\n    return session.sql("SELECT * FROM schema.table")',
                "tags": [],
            },
        )


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


class TestCheckModelIncrementalHasUniqueKey:
    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"config": {"materialized": "incremental", "unique_key": "id"}},
                id="incremental_with_unique_key",
            ),
            pytest.param(
                {"config": {"materialized": "view"}},
                id="view_no_unique_key",
            ),
            pytest.param(
                {"config": {"materialized": "table"}},
                id="table_no_unique_key",
            ),
        ],
    )
    def test_pass(self, model_override):
        check_passes("check_model_incremental_has_unique_key", model=model_override)

    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {"config": {"materialized": "incremental"}},
                id="incremental_no_unique_key",
            ),
            pytest.param(
                {"config": {"materialized": "incremental", "unique_key": ""}},
                id="incremental_empty_unique_key",
            ),
        ],
    )
    def test_fail(self, model_override):
        check_fails("check_model_incremental_has_unique_key", model=model_override)


class TestCheckModelMaterializationPermitted:
    @pytest.mark.parametrize(
        ("model_override", "permitted_materializations"),
        [
            pytest.param(
                {"config": {"materialized": "view"}},
                ["view"],
                id="view_permitted_view",
            ),
            pytest.param(
                {"config": {"materialized": "table"}},
                ["table"],
                id="table_permitted_table",
            ),
            pytest.param(
                {"config": {"materialized": "ephemeral"}},
                ["ephemeral", "view"],
                id="ephemeral_permitted_multiple",
            ),
        ],
    )
    def test_pass(self, model_override, permitted_materializations):
        check_passes(
            "check_model_materialization_permitted",
            model=model_override,
            permitted_materializations=permitted_materializations,
        )

    @pytest.mark.parametrize(
        ("model_override", "permitted_materializations"),
        [
            pytest.param(
                {"config": {"materialized": "table"}},
                ["view"],
                id="table_overridden_from_view",
            ),
            pytest.param(
                {"config": {"materialized": "incremental"}},
                ["view", "table"],
                id="incremental_not_permitted",
            ),
            pytest.param(
                {"config": None},
                ["view"],
                id="none_config",
            ),
        ],
    )
    def test_fail(self, model_override, permitted_materializations):
        check_fails(
            "check_model_materialization_permitted",
            model=model_override,
            permitted_materializations=permitted_materializations,
        )


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

        with pytest.raises(ValueError, match="greater than 0"):
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
