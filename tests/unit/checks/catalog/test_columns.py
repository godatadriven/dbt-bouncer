import warnings
from contextlib import nullcontext as does_not_raise

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.catalog.catalog_v1 import Nodes as CatalogNodes

    from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
        ManifestLatest,
        Metadata,
    )

from pydantic import ValidationError

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import Nodes4, Nodes6
from dbt_bouncer.artifact_parsers.parsers_manifest import (  # noqa: F401  # noqa: F401
    DbtBouncerManifest,
    DbtBouncerModelBase,
    DbtBouncerTest,
    DbtBouncerTestBase,
)
from dbt_bouncer.checks.catalog.check_columns import (
    CheckColumnDescriptionPopulated,
    CheckColumnHasSpecifiedTest,
    CheckColumnNameCompliesToColumnType,
    CheckColumnNames,
    CheckColumnsAreAllDocumented,
    CheckColumnsAreDocumentedInPublicModels,
)
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError

CheckColumnDescriptionPopulated.model_rebuild()
CheckColumnNameCompliesToColumnType.model_rebuild()
CheckColumnNames.model_rebuild()
CheckColumnsAreAllDocumented.model_rebuild()
CheckColumnsAreDocumentedInPublicModels.model_rebuild()
CheckColumnHasSpecifiedTest.model_rebuild()


@pytest.fixture
def catalog_node(request):
    default_catalog_node = {
        "columns": {
            "col_1": {
                "index": 1,
                "name": "col_1",
                "type": "INTEGER",
            },
            "col_2": {
                "index": 2,
                "name": "col_2",
                "type": "INTEGER",
            },
        },
        "metadata": {
            "name": "table_1",
            "schema": "main",
            "type": "VIEW",
        },
        "stats": {},
        "unique_id": "model.package_name.model_1",
    }
    return CatalogNodes(**{**default_catalog_node, **getattr(request, "param", {})})


@pytest.fixture
def models(request):
    default_model = {
        "alias": "model_1",
        "checksum": {"name": "sha256", "checksum": ""},
        "columns": {
            "col_1": {
                "description": "This is a description",
                "index": 1,
                "name": "col_1",
                "type": "INTEGER",
            },
            "col_2": {
                "description": "This is a description",
                "index": 2,
                "name": "col_2",
                "type": "INTEGER",
            },
        },
        "fqn": ["package_name", "model_1"],
        "name": "model_1",
        "original_file_path": "model_1.sql",
        "package_name": "package_name",
        "path": "model_1.sql",
        "resource_type": "model",
        "schema": "main",
        "unique_id": "model.package_name.model_1",
    }
    return [Nodes4(**{**default_model, **getattr(request, "param", {})})]


@pytest.fixture
def tests(request):
    default_test = {
        "alias": "not_null_model_1_unique",
        "attached_node": "model.package_name.model_1",
        "checksum": {"name": "none", "checksum": ""},
        "column_name": "col_1",
        "fqn": [
            "package_name",
            "marts",
            "finance",
            "not_null_model_1_unique",
        ],
        "name": "not_null_model_1_unique",
        "original_file_path": "models/marts/finance/_finance__models.yml",
        "package_name": "package_name",
        "path": "not_null_model_1_unique.sql",
        "resource_type": "test",
        "schema": "main",
        "test_metadata": {
            "name": "unique",
        },
        "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
    }
    return [Nodes6(**{**default_test, **getattr(request, "param", {})})]


@pytest.fixture
def manifest_obj(request):
    default_manifest = {
        "metadata": Metadata(
            dbt_schema_version="https://schemas.getdbt.com/dbt/manifest/v12.json",
            dbt_version="1.11.0a1",
            generated_at=None,
            invocation_id=None,
            invocation_started_at=None,
            env=None,
            project_name="dbt_bouncer_test_project",
            project_id=None,
            user_id=None,
            send_anonymous_usage_stats=None,
            adapter_type="postgres",
            quoting=None,
            run_started_at=None,
        ),
        "nodes": {},
        "sources": {},
        "macros": {},
        "docs": {},
        "exposures": {},
        "metrics": {},
        "groups": {},
        "selectors": {},
        "disabled": {},
        "parent_map": {},
        "child_map": {},
        "group_map": {},
        "saved_queries": {},
        "semantic_models": {},
        "unit_tests": {},
        "functions": None,
    }

    # Handle DbtBouncerManifest wrapping
    params = getattr(request, "param", {})
    if params == "manifest_obj":  # Default marker used in existing tests
        params = {}

    # If params provides ManifestLatest, use it, else create one
    if "manifest" in params:
        return DbtBouncerManifest(**params)

    adapter_type = params.pop("adapter_type", None)
    if adapter_type:
        default_manifest["metadata"] = default_manifest["metadata"].model_copy(
            update={"adapter_type": adapter_type}
        )

    manifest_data = {**default_manifest, **params}
    return DbtBouncerManifest(manifest=ManifestLatest(**manifest_data))


_TEST_DATA_FOR_CHECK_COLUMN_DESCRIPTION_POPULATED = [
    pytest.param(
        {},
        {},
        does_not_raise(),
        id="all_documented",
    ),
    pytest.param(
        {
            "unique_id": "model.package_name.model_2",
        },
        {
            "alias": "model_2",
            "columns": {
                "col_1": {
                    "description": "This is a description",
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
                "col_2": {
                    "index": 2,
                    "name": "col_2",
                    "type": "INTEGER",
                },
            },
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_documentation",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMN_DESCRIPTION_POPULATED,
    indirect=["catalog_node", "models"],
)
def test_check_column_description_populated(catalog_node, models, expectation):
    with expectation:
        CheckColumnDescriptionPopulated(
            catalog_node=catalog_node,
            models=models,
            name="check_column_description_populated",
        ).execute()


_TEST_DATA_FOR_CHECK_COLUMN_HAS_SPECIFIED_TEST = [
    pytest.param(
        {
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
            },
        },
        ".*_1$",
        "unique",
        {},
        does_not_raise(),
        id="has_test",
    ),
    pytest.param(
        {
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
            },
        },
        ".*_1$",
        "unique",
        {
            "alias": "not_null_model_1_not_null",
            "fqn": [
                "package_name",
                "marts",
                "finance",
                "not_null_model_1_not_null",
            ],
            "name": "not_null_model_1_not_null",
            "test_metadata": {
                "name": "not_null",
            },
            "unique_id": "test.package_name.not_null_model_1_not_null.cf6c17daed",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_test",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "column_name_pattern", "test_name", "tests", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMN_HAS_SPECIFIED_TEST,
    indirect=["catalog_node", "tests"],
)
def test_check_column_has_specified_test(
    catalog_node,
    column_name_pattern,
    test_name,
    tests,
    expectation,
):
    with expectation:
        CheckColumnHasSpecifiedTest(
            catalog_node=catalog_node,
            column_name_pattern=column_name_pattern,
            name="check_column_has_specified_test",
            test_name=test_name,
            tests=tests,
        ).execute()


_TEST_DATA_FOR_CHECK_COLUMNS_ARE_ALL_DOCUMENTED = [
    pytest.param(
        {},
        {},
        {},
        does_not_raise(),
        id="all_documented",
    ),
    pytest.param(
        {},
        {},
        {
            "columns": {
                "COL_1": {
                    "index": 1,
                    "name": "COL_1",
                    "type": "INTEGER",
                },
                "COL_2": {
                    "index": 2,
                    "name": "COL_2",
                    "type": "INTEGER",
                },
            },
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="case_mismatch",
    ),
    pytest.param(
        {
            "unique_id": "model.package_name.model_2",
        },
        {},
        {
            "alias": "model_2",
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
            },
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_column_in_model",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "manifest_obj", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMNS_ARE_ALL_DOCUMENTED,
    indirect=["catalog_node", "manifest_obj", "models"],
)
def test_check_columns_are_all_documented(
    catalog_node, manifest_obj, models, expectation
):
    with expectation:
        CheckColumnsAreAllDocumented(
            catalog_node=catalog_node,
            manifest_obj=manifest_obj,
            models=models,
            name="check_columns_are_all_documented",
        ).execute()


_TEST_DATA_FOR_CHECK_COLUMNS_ARE_ALL_DOCUMENTED_SNOWFLAKE = [
    pytest.param(
        {},
        {"adapter_type": "snowflake"},
        {
            "columns": {
                "COL_1": {
                    "index": 1,
                    "name": "COL_1",
                    "type": "INTEGER",
                },
                "COL_2": {
                    "index": 2,
                    "name": "COL_2",
                    "type": "INTEGER",
                },
            },
        },
        does_not_raise(),
        id="all_documented_snowflake",
    )
]


@pytest.mark.parametrize(
    ("catalog_node", "manifest_obj", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMNS_ARE_ALL_DOCUMENTED_SNOWFLAKE,
    indirect=["catalog_node", "manifest_obj", "models"],
)
def test_check_columns_are_all_documented_snowflake(
    catalog_node, manifest_obj, models, expectation
):
    with expectation:
        CheckColumnsAreAllDocumented(
            catalog_node=catalog_node,
            manifest_obj=manifest_obj,
            models=models,
            name="check_columns_are_all_documented",
        ).execute()


_TEST_DATA_FOR_CHECK_COLUMNS_ARE_DOCUMENTED_IN_PUBLIC_MODELS = [
    pytest.param(
        {
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
            },
        },
        {
            "access": "public",
            "columns": {
                "col_1": {
                    "description": "This is a description",
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
            },
        },
        does_not_raise(),
        id="documented_public",
    ),
    pytest.param(
        {},
        {
            "access": "public",
            "columns": {
                "col_1": {
                    "description": "This is a description",
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
                "col_2": {
                    "description": "",
                    "index": 2,
                    "name": "col_2",
                    "type": "INTEGER",
                },
            },
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="undocumented_public",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMNS_ARE_DOCUMENTED_IN_PUBLIC_MODELS,
    indirect=["catalog_node", "models"],
)
def test_check_columns_are_documented_in_public_models(
    catalog_node,
    models,
    expectation,
):
    with expectation:
        CheckColumnsAreDocumentedInPublicModels(
            catalog_node=catalog_node,
            models=models,
            name="check_columns_are_documented_in_public_models",
        ).execute()


_TEST_DATA_FOR_CHECK_COLUMN_NAME_COMPLIES_TO_COLUMN_TYPE = [
    pytest.param(
        {
            "columns": {
                "col_1_date": {
                    "index": 1,
                    "name": "col_1_date",
                    "type": "DATE",
                },
            },
        },
        ".*_date$",
        None,
        ["DATE"],
        does_not_raise(),
        id="valid_date",
    ),
    pytest.param(
        {
            "columns": {
                "col_1_date": {
                    "index": 1,
                    "name": "col_1_date",
                    "type": "DATE",
                },
                "col_2_date": {
                    "index": 2,
                    "name": "col_2_date",
                    "type": "DATE",
                },
                "col_3": {
                    "index": 3,
                    "name": "col_3",
                    "type": "VARCHAR",
                },
            },
        },
        ".*_date$",
        None,
        ["DATE"],
        does_not_raise(),
        id="valid_dates",
    ),
    pytest.param(
        {
            "columns": {
                "col_1_date": {
                    "index": 1,
                    "name": "col_1_date",
                    "type": "DATE",
                },
                "col_2_date": {
                    "index": 2,
                    "name": "col_2_date",
                    "type": "STRUCT",
                },
                "col_3": {
                    "index": 3,
                    "name": "col_3",
                    "type": "VARCHAR",
                },
            },
        },
        ".*_date$",
        "^(?!STRUCT)",
        None,
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_struct",
    ),
    pytest.param(
        {
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "DATE",
                },
            },
        },
        ".*_date$",
        None,
        ["DATE"],
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_name",
    ),
    pytest.param(
        {
            "columns": {
                "col_1_date": {
                    "index": 1,
                    "name": "col_1_date",
                    "type": "DATE",
                },
                "col_2_date": {
                    "index": 2,
                    "name": "col_2_date",
                    "type": "DATE",
                },
                "col_3": {
                    "index": 3,
                    "name": "col_3",
                    "type": "VARCHAR",
                },
            },
        },
        ".*_date$",
        None,
        None,
        pytest.raises(ValidationError),
        id="missing_pattern_and_types",
    ),
    pytest.param(
        {
            "columns": {
                "col_1_date": {
                    "index": 1,
                    "name": "col_1_date",
                    "type": "DATE",
                },
                "col_2_date": {
                    "index": 2,
                    "name": "col_2_date",
                    "type": "DATE",
                },
                "col_3": {
                    "index": 3,
                    "name": "col_3",
                    "type": "VARCHAR",
                },
            },
        },
        ".*_date$",
        "^(?!STRUCT)",
        ["DATE"],
        pytest.raises(ValidationError),
        id="both_pattern_and_types",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "column_name_pattern", "type_pattern", "types", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMN_NAME_COMPLIES_TO_COLUMN_TYPE,
    indirect=["catalog_node"],
)
def test_check_column_name_complies_to_column_type(
    catalog_node,
    column_name_pattern,
    type_pattern,
    types,
    expectation,
):
    with expectation:
        CheckColumnNameCompliesToColumnType(
            catalog_node=catalog_node,
            column_name_pattern=column_name_pattern,
            name="check_column_name_complies_to_column_type",
            type_pattern=type_pattern,
            types=types,
        ).execute()


_TEST_DATA_FOR_CHECK_COLUMN_NAMES = [
    pytest.param(
        {
            "columns": {
                "columnone": {
                    "index": 1,
                    "name": "columnone",
                    "type": "DATE",
                },
            },
        },
        "[a-z]*",
        {
            "columns": {
                "columnone": {
                    "description": None,
                    "index": 1,
                    "name": "columnone",
                    "type": "INTEGER",
                },
            },
        },
        does_not_raise(),
        id="valid_name",
    ),
    pytest.param(
        {
            "columns": {
                "columnone": {
                    "index": 1,
                    "name": "columnone",
                    "type": "DATE",
                },
            },
        },
        "[A-Z]*",
        {
            "columns": {
                "columnone": {
                    "description": None,
                    "index": 1,
                    "name": "columnone",
                    "type": "INTEGER",
                },
            },
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_name",
    ),
]


@pytest.mark.parametrize(
    ("catalog_node", "column_name_pattern", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_COLUMN_NAMES,
    indirect=["catalog_node", "models"],
)
def test_check_column_names(
    catalog_node,
    column_name_pattern,
    models,
    expectation,
):
    with expectation:
        CheckColumnNames(
            catalog_node=catalog_node,
            column_name_pattern=column_name_pattern,
            models=models,
            name="check_column_names",
        ).execute()
