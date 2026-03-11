from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.parser import wrap_dict
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.models.code import (
    CheckModelCodeDoesNotContainRegexpPattern,
    CheckModelHasSemiColon,
    CheckModelMaxNumberOfLines,
)

_TEST_DATA_FOR_CHECK_MODEL_CODE_DOES_NOT_CONTAIN_REGEXP_PATTERN = [
    pytest.param(
        {
            "raw_code": "select coalesce(a, b) from table",
        },
        ".*[i][f][n][u][l][l].*",
        does_not_raise(),
        id="does_not_contain_pattern",
    ),
    pytest.param(
        {
            "raw_code": "select ifnull(a, b) from table",
        },
        ".*[i][f][n][u][l][l].*",
        pytest.raises(DbtBouncerFailedCheckError),
        id="contains_pattern",
    ),
]


@pytest.mark.parametrize(
    ("model", "regexp_pattern", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_CODE_DOES_NOT_CONTAIN_REGEXP_PATTERN,
    indirect=["model"],
)
def test_check_model_code_does_not_contain_regexp_pattern(
    model,
    regexp_pattern,
    expectation,
):
    with expectation:
        CheckModelCodeDoesNotContainRegexpPattern(
            model=model,
            name="check_model_code_does_not_contain_regexp_pattern",
            regexp_pattern=regexp_pattern,
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_SEMI_COLON = [
    pytest.param(
        {
            "raw_code": "select 1 as id",
            "tags": [],
        },
        does_not_raise(),
        id="no_semicolon",
    ),
    pytest.param(
        {
            "raw_code": """select 1 as id
                    """,
            "tags": [],
        },
        does_not_raise(),
        id="multiline_no_semicolon",
    ),
    pytest.param(
        {
            "raw_code": """-- comment with ;
                    select 1 as id""",
            "tags": [],
        },
        does_not_raise(),
        id="semicolon_in_comment",
    ),
    pytest.param(
        {
            "raw_code": "select 1 as id;",
            "tags": [],
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="semicolon",
    ),
    pytest.param(
        {
            "raw_code": "select 1 as id; ",
            "tags": [],
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="semicolon_with_space",
    ),
    pytest.param(
        {
            "raw_code": """select 1 as id;

                    """,
            "tags": [],
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="multiline_semicolon",
    ),
    pytest.param(
        {
            "raw_code": """select 1 as id
                    ; """,
            "tags": [],
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="multiline_semicolon_next_line",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_SEMI_COLON,
    indirect=["model"],
)
def test_check_model_has_semi_colon(model, expectation):
    with expectation:
        CheckModelHasSemiColon(
            model=model,
            name="check_model_has_semi_colon",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_MAX_NUMBER_OF_LINES = [
    pytest.param(
        100,
        {
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "raw_code": 'with\n    source as (\n\n        {#-\n    Normally we would select from the table here, but we are using seeds to load\n    our data in this project\n    #}\n        select * from {{ ref("raw_orders") }}\n\n    ),\n\n    renamed as (\n\n        select id as order_id, user_id as customer_id, order_date, status from source\n\n    )\n\nselect *\nfrom renamed',
        },
        does_not_raise(),
        id="within_limit",
    ),
    pytest.param(
        10,
        {
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_schema.yml",
            "path": "staging/crm/stg_model_1.sql",
            "raw_code": 'with\n    source as (\n\n        {#-\n    Normally we would select from the table here, but we are using seeds to load\n    our data in this project\n    #}\n        select * from {{ ref("raw_orders") }}\n\n    ),\n\n    renamed as (\n\n        select id as order_id, user_id as customer_id, order_date, status from source\n\n    )\n\nselect *\nfrom renamed',
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_limit",
    ),
]


@pytest.mark.parametrize(
    ("max_number_of_lines", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_MAX_NUMBER_OF_LINES,
    indirect=["model"],
)
def test_check_model_max_number_of_lines(max_number_of_lines, model, expectation):
    with expectation:
        CheckModelMaxNumberOfLines(
            max_number_of_lines=max_number_of_lines,
            model=model,
            name="check_model_max_number_of_lines",
        ).execute()


# Python Model Tests


_TEST_DATA_FOR_PYTHON_MODEL_SEMICOLON = [
    pytest.param(
        {
            "language": "python",
            "raw_code": """def model(dbt, session):
    df = dbt.ref("upstream")
    return df""",
            "tags": [],
        },
        does_not_raise(),
        id="python_no_semicolon",
    ),
    pytest.param(
        {
            "language": "python",
            "raw_code": """def model(dbt, session):
    return dbt.ref("upstream");""",
            "tags": [],
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="python_with_semicolon",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_PYTHON_MODEL_SEMICOLON,
    indirect=["model"],
)
def test_check_python_model_has_semi_colon(model, expectation):
    """Test that Python models are checked for trailing semicolons."""
    with expectation:
        CheckModelHasSemiColon(
            model=model,
            name="check_model_has_semi_colon",
        ).execute()


_TEST_DATA_FOR_PYTHON_MODEL_REGEXP = [
    pytest.param(
        """import pandas as pd

def model(dbt, session):
    df = dbt.ref("upstream")
    return df""",
        r"import\s+os",
        does_not_raise(),
        id="python_no_forbidden_import",
    ),
    pytest.param(
        """import os
import pandas as pd

def model(dbt, session):
    df = dbt.ref("upstream")
    return df""",
        r"import\s+os",
        pytest.raises(DbtBouncerFailedCheckError),
        id="python_forbidden_import",
    ),
    pytest.param(
        """def model(dbt, session):
    df = dbt.ref("large_table").to_pandas()
    return df""",
        r".*\.to_pandas\(\)",
        pytest.raises(DbtBouncerFailedCheckError),
        id="python_forbidden_method",
    ),
]


@pytest.mark.parametrize(
    ("raw_code", "regexp_pattern", "expectation"),
    _TEST_DATA_FOR_PYTHON_MODEL_REGEXP,
)
def test_check_python_model_code_does_not_contain_regexp_pattern(
    raw_code, regexp_pattern, expectation
):
    """Test that regexp pattern checking works with Python models."""
    model = wrap_dict(
        {
            "alias": "test_model",
            "checksum": {"name": "sha256", "checksum": "abc123"},
            "columns": {},
            "config": {},
            "depends_on": {"macros": [], "nodes": []},
            "fqn": ["project", "test_model"],
            "language": "python",
            "name": "test_model",
            "original_file_path": "models/test_model.py",
            "package_name": "project",
            "path": "test_model.py",
            "raw_code": raw_code,
            "refs": [],
            "resource_type": "model",
            "schema": "main",
            "sources": [],
            "unique_id": "model.project.test_model",
        }
    )

    with expectation:
        CheckModelCodeDoesNotContainRegexpPattern(
            model=model,
            name="check_model_code_does_not_contain_regexp_pattern",
            regexp_pattern=regexp_pattern,
        ).execute()


_TEST_DATA_FOR_PYTHON_MODEL_MAX_LINES = [
    pytest.param(
        {
            "language": "python",
            "raw_code": """import pandas as pd

def model(dbt, session):
    df = dbt.ref("upstream")
    return df""",
            "tags": [],
        },
        10,
        does_not_raise(),
        id="python_under_limit",
    ),
    pytest.param(
        {
            "language": "python",
            "raw_code": """import pandas as pd
# Line 2
# Line 3
# Line 4
# Line 5
# Line 6
# Line 7
# Line 8
# Line 9
# Line 10
# Line 11

def model(dbt, session):
    df = dbt.ref("upstream")
    return df""",
            "tags": [],
        },
        10,
        pytest.raises(DbtBouncerFailedCheckError),
        id="python_over_limit",
    ),
]


@pytest.mark.parametrize(
    ("model", "max_number_of_lines", "expectation"),
    _TEST_DATA_FOR_PYTHON_MODEL_MAX_LINES,
    indirect=["model"],
)
def test_check_python_model_max_number_of_lines(
    model, max_number_of_lines, expectation
):
    """Test that line counting works with Python models."""
    with expectation:
        CheckModelMaxNumberOfLines(
            max_number_of_lines=max_number_of_lines,
            model=model,
            name="check_model_max_number_of_lines",
        ).execute()
