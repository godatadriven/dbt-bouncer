from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.artifact_parsers.dbt_cloud.manifest_latest import (
    Exposures,
    ManifestLatest,
    Metadata,
    Nodes4,
    Nodes6,
    UnitTests,
)
from dbt_bouncer.artifact_parsers.parsers_manifest import DbtBouncerManifest
from dbt_bouncer.checks.common import DbtBouncerFailedCheckError
from dbt_bouncer.checks.manifest.check_models import (
    CheckModelAccess,
    CheckModelCodeDoesNotContainRegexpPattern,
    CheckModelContractsEnforcedForPublicModel,
    CheckModelDependsOnMacros,
    CheckModelDependsOnMultipleSources,
    CheckModelDescriptionContainsRegexPattern,
    CheckModelDescriptionPopulated,
    CheckModelDirectories,
    CheckModelDocumentedInSameDirectory,
    CheckModelFileName,
    CheckModelGrantPrivilege,
    CheckModelGrantPrivilegeRequired,
    CheckModelHasContractsEnforced,
    CheckModelHasExposure,
    CheckModelHasMetaKeys,
    CheckModelHasNoUpstreamDependencies,
    CheckModelHasSemiColon,
    CheckModelHasTags,
    CheckModelHasUniqueTest,
    CheckModelHasUnitTests,
    CheckModelLatestVersionSpecified,
    CheckModelMaxChainedViews,
    CheckModelMaxFanout,
    CheckModelMaxNumberOfLines,
    CheckModelMaxUpstreamDependencies,
    CheckModelNames,
    CheckModelNumberOfGrants,
    CheckModelPropertyFileLocation,
    CheckModelSchemaName,
    CheckModelsDocumentationCoverage,
    CheckModelsTestCoverage,
    CheckModelVersionAllowed,
    CheckModelVersionPinnedInRef,
)


@pytest.fixture
def model(request):
    default_model = {
        "alias": "model_1",
        "checksum": {"name": "sha256", "checksum": ""},
        "columns": {
            "col_1": {
                "index": 1,
                "name": "col_1",
                "type": "INTEGER",
            },
        },
        "fqn": ["package_name", "model_1"],
        "name": "model_1",
        "original_file_path": "model_1.sql",
        "package_name": "package_name",
        "path": "staging/finance/model_1.sql",
        "resource_type": "model",
        "schema": "main",
        "unique_id": "model.package_name.model_1",
    }
    return Nodes4(**{**default_model, **getattr(request, "param", {})})


@pytest.fixture
def models(request):
    default_model = {
        "alias": "model_1",
        "checksum": {"name": "sha256", "checksum": ""},
        "columns": {
            "col_1": {
                "index": 1,
                "name": "col_1",
                "type": "INTEGER",
            },
        },
        "fqn": ["package_name", "model_1"],
        "name": "model_1",
        "original_file_path": "model_1.sql",
        "package_name": "package_name",
        "path": "staging/finance/model_1.sql",
        "resource_type": "model",
        "schema": "main",
        "unique_id": "model.package_name.model_1",
    }
    params = getattr(request, "param", [])
    return [Nodes4(**{**default_model, **p}) for p in params]


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
    params = getattr(request, "param", [])
    return [Nodes6(**{**default_test, **p}) for p in params]


@pytest.fixture
def exposures(request):
    default_exposure = {
        "depends_on": {"nodes": ["model.package_name.model_1"]},
        "fqn": ["package_name", "marts", "finance", "exposure_1"],
        "name": "exposure_1",
        "original_file_path": "models/marts/finance/_exposures.yml",
        "owner": {
            "email": "anna.anderson@example.com",
            "name": "Anna Anderson",
        },
        "package_name": "package_name",
        "path": "marts/finance/_exposures.yml",
        "resource_type": "exposure",
        "type": "dashboard",
        "unique_id": "exposure.package_name.exposure_1",
    }
    params = getattr(request, "param", [])
    return [Exposures(**{**default_exposure, **p}) for p in params]


@pytest.fixture
def unit_tests(request):
    default_unit_test = {
        "depends_on": {
            "nodes": [
                "model.package_name.model_1",
            ],
        },
        "expect": {"format": "dict", "rows": [{"id": 1}]},
        "fqn": [
            "package_name",
            "staging",
            "crm",
            "model_1",
            "unit_test_1",
        ],
        "given": [{"input": "ref(input_1)", "format": "csv"}],
        "model": "model_1",
        "name": "unit_test_1",
        "original_file_path": "models/staging/crm/_crm__source.yml",
        "resource_type": "unit_test",
        "package_name": "package_name",
        "path": "staging/crm/_crm__source.yml",
        "unique_id": "unit_test.package_name.model_1.unit_test_1",
    }
    params = getattr(request, "param", [])
    return [UnitTests(**{**default_unit_test, **p}) for p in params]


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

    params = getattr(request, "param", {})
    if params == "manifest_obj":
        params = {}

    if "manifest" in params:
        manifest_data = {**default_manifest, **params["manifest"]}
        return DbtBouncerManifest(manifest=ManifestLatest(**manifest_data))

    manifest_data = {**default_manifest, **params}
    return DbtBouncerManifest(manifest=ManifestLatest(**manifest_data))


_TEST_DATA_FOR_CHECK_MODEL_ACCESS = [
    pytest.param(
        "public",
        {
            "access": "public",
            "alias": "model_2",
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        does_not_raise(),
        id="public_access",
    ),
    pytest.param(
        "public",
        {
            "access": "protected",
            "alias": "model_2",
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="protected_access",
    ),
]


@pytest.mark.parametrize(
    ("access", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_ACCESS,
    indirect=["model"],
)
def test_check_model_access(access, model, expectation):
    with expectation:
        CheckModelAccess(
            access=access, model=model, name="check_model_access"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_CONTRACT_ENFORCED_FOR_PUBLIC_MODEL = [
    pytest.param(
        {
            "access": "public",
            "alias": "model_2",
            "contract": {"enforced": True},
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        does_not_raise(),
        id="public_contract_enforced",
    ),
    pytest.param(
        {
            "access": "protected",
            "alias": "model_2",
            "contract": {"enforced": False},
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        does_not_raise(),
        id="protected_no_contract",
    ),
    pytest.param(
        {
            "access": "public",
            "alias": "model_2",
            "contract": {"enforced": False},
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="public_no_contract",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_CONTRACT_ENFORCED_FOR_PUBLIC_MODEL,
    indirect=["model"],
)
def test_check_model_contract_enforced_for_public_model(model, expectation):
    with expectation:
        CheckModelContractsEnforcedForPublicModel(
            model=model, name="check_model_contract_enforced_for_public_model"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_DEPENDS_ON_MACROS = [
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {"macros": ["macro.dbt.is_incremental"]},
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.is_incremental"],
        "all",
        does_not_raise(),
        id="depends_on_required_macro",
    ),
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {"macros": ["macro.dbt.is_incremental"]},
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.is_incremental", "dbt.other_macro"],
        "all",
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_one_required_macro",
    ),
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {
                "macros": [
                    "macro.dbt.is_incremental",
                    "macro.dbt.other_macro",
                ]
            },
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.is_incremental"],
        "any",
        does_not_raise(),
        id="depends_on_any_macro",
    ),
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {"macros": ["macro.dbt.is_incremental"]},
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.other_macro"],
        "any",
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_any_required_macro",
    ),
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {"macros": ["macro.dbt.is_incremental"]},
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.is_incremental", "dbt.other_macro"],
        "one",
        does_not_raise(),
        id="depends_on_one_macro",
    ),
    pytest.param(
        {
            "unique_id": "model.package.model_1",
            "depends_on": {
                "macros": [
                    "macro.dbt.is_incremental",
                    "macro.dbt.other_macro",
                ]
            },
            "resource_type": "model",
            "path": "model_1.sql",
            "original_file_path": "model_1.sql",
            "package_name": "package",
            "name": "model_1",
            "schema": "schema",
            "alias": "model_1",
            "fqn": ["package", "model_1"],
            "checksum": {"name": "sha256", "checksum": "checksum"},
        },
        ["dbt.is_incremental", "dbt.other_macro"],
        "one",
        pytest.raises(DbtBouncerFailedCheckError),
        id="depends_on_too_many_macros",
    ),
]


@pytest.mark.parametrize(
    ("model", "required_macros", "criteria", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DEPENDS_ON_MACROS,
    indirect=["model"],
)
def test_check_model_depends_on_macros(model, required_macros, criteria, expectation):
    with expectation:
        CheckModelDependsOnMacros(
            model=model,
            required_macros=required_macros,
            criteria=criteria,
            name="check_model_depends_on_macros",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_DEPENDS_ON_MULTIPLE_SOURCES = [
    pytest.param(
        {
            "alias": "model_2",
            "depends_on": {"nodes": ["source.package_name.source_1"]},
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        does_not_raise(),
        id="single_source_dependency",
    ),
    pytest.param(
        {
            "alias": "model_2",
            "depends_on": {
                "nodes": [
                    "source.package_name.source_1",
                    "source.package_name.source_2",
                ],
            },
            "fqn": ["package_name", "model_2"],
            "name": "model_2",
            "original_file_path": "model_2.sql",
            "path": "model_2.sql",
            "unique_id": "model.package_name.model_2",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="multiple_source_dependency",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DEPENDS_ON_MULTIPLE_SOURCES,
    indirect=["model"],
)
def test_check_model_depends_on_multiple_sources(model, expectation):
    with expectation:
        CheckModelDependsOnMultipleSources(
            model=model, name="check_model_depends_on_multiple_sources"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_DOCUMENTATION_COVERAGE = [
    pytest.param(
        100,
        [
            {
                "alias": "model_2",
                "description": "Model 2 description",
                "fqn": ["package_name", "model_2"],
                "name": "model_2",
                "original_file_path": "model_2.sql",
                "path": "model_2.sql",
                "unique_id": "model.package_name.model_2",
            },
        ],
        does_not_raise(),
        id="100_percent_coverage",
    ),
    pytest.param(
        50,
        [
            {
                "alias": "model_1",
                "description": "Model 1 description",
                "fqn": ["package_name", "model_1"],
                "name": "model_1",
                "original_file_path": "model_1.sql",
                "path": "model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            {
                "alias": "model_2",
                "description": "",
                "fqn": ["package_name", "model_2"],
                "name": "model_2",
                "original_file_path": "model_2.sql",
                "path": "model_2.sql",
                "unique_id": "model.package_name.model_2",
            },
        ],
        does_not_raise(),
        id="50_percent_coverage",
    ),
    pytest.param(
        100,
        [
            {
                "alias": "model_2",
                "description": "",
                "fqn": ["package_name", "model_2"],
                "name": "model_2",
                "original_file_path": "model_2.sql",
                "path": "model_2.sql",
                "unique_id": "model.package_name.model_2",
            },
        ],
        pytest.raises(DbtBouncerFailedCheckError),
        id="0_percent_coverage",
    ),
]


@pytest.mark.parametrize(
    ("min_model_documentation_coverage_pct", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DOCUMENTATION_COVERAGE,
    indirect=["models"],
)
def test_check_model_documentation_coverage(
    min_model_documentation_coverage_pct,
    models,
    expectation,
):
    with expectation:
        CheckModelsDocumentationCoverage(
            min_model_documentation_coverage_pct=min_model_documentation_coverage_pct,
            models=models,
            name="check_model_documentation_coverage",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_DOCUMENTED_IN_SAME_DIRECTORY = [
    pytest.param(
        {
            "alias": "model_1",
            "fqn": ["package_name", "model_1"],
            "name": "model_1",
            "original_file_path": "models/staging/model_1.sql",
            "patch_path": "package_name://models/staging/_schema.yml",
            "path": "staging/model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        does_not_raise(),
        id="same_directory",
    ),
    pytest.param(
        {
            "alias": "model_1",
            "fqn": ["package_name", "model_1"],
            "name": "model_1",
            "original_file_path": "models/staging/finance/model_1.sql",
            "patch_path": "package_name://models/staging/_schema.yml",
            "path": "staging/finance/model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="different_directory",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DOCUMENTED_IN_SAME_DIRECTORY,
    indirect=["model"],
)
def test_check_model_documented_in_same_directory(model, expectation):
    with expectation:
        CheckModelDocumentedInSameDirectory(
            model=model, name="check_model_documented_in_same_directory"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_FILE_NAMES = [
    pytest.param(
        r".*(v[0-9])\.sql$",
        {
            "alias": "model_v1",
            "config": {"grants": {"select": ["user1"]}},
            "fqn": ["package_name", "model_v1"],
            "name": "model_v1",
            "original_file_path": "model_v1.sql",
            "path": "staging/finance/model_v1.sql",
            "unique_id": "model.package_name.model_v1",
        },
        does_not_raise(),
        id="valid_file_name",
    ),
    pytest.param(
        ".*(v[0-9])$",
        {
            "alias": "model_v1",
            "config": {"grants": {"select": ["user1"]}},
            "fqn": ["package_name", "model_v1"],
            "name": "model_v1",
            "original_file_path": "model_v1.sql",
            "path": "staging/finance/model_v1.sql",
            "unique_id": "model.package_name.model_v1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_file_name",
    ),
]


@pytest.mark.parametrize(
    ("file_name_pattern", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_FILE_NAMES,
    indirect=["model"],
)
def test_check_model_file_names(file_name_pattern, model, expectation):
    with expectation:
        CheckModelFileName(
            file_name_pattern=file_name_pattern,
            model=model,
            name="check_model_file_name",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_GRANT_PRIVILEGE = [
    pytest.param(
        "select",
        {
            "config": {"grants": {"select": ["user1"]}},
        },
        does_not_raise(),
        id="grant_select",
    ),
    pytest.param(
        "^select$",
        {
            "config": {"grants": {"write": ["user1"]}},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="grant_write",
    ),
]


@pytest.mark.parametrize(
    ("privilege_pattern", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_GRANT_PRIVILEGE,
    indirect=["model"],
)
def test_check_model_grant_privilege(privilege_pattern, model, expectation):
    with expectation:
        CheckModelGrantPrivilege(
            privilege_pattern=privilege_pattern,
            model=model,
            name="check_model_grant_privilege",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_GRANT_PRIVILEGE_REQUIRED = [
    pytest.param(
        "select",
        {
            "config": {"grants": {"select": ["user1"]}},
        },
        does_not_raise(),
        id="required_grant_present",
    ),
    pytest.param(
        "select",
        {
            "config": {"grants": {"write": ["user1"]}},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="required_grant_missing",
    ),
]


@pytest.mark.parametrize(
    ("privilege", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_GRANT_PRIVILEGE_REQUIRED,
    indirect=["model"],
)
def test_check_model_grant_privilege_required(privilege, model, expectation):
    with expectation:
        CheckModelGrantPrivilegeRequired(
            privilege=privilege,
            model=model,
            name="check_model_grant_privilege_required",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_CONTRACTS_ENFORCED = [
    pytest.param(
        {
            "contract": {"enforced": True},
        },
        does_not_raise(),
        id="contracts_enforced",
    ),
    pytest.param(
        {
            "contract": {"enforced": False},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="contracts_not_enforced",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_CONTRACTS_ENFORCED,
    indirect=["model"],
)
def test_check_model_has_contracts_enforced(model, expectation):
    with expectation:
        CheckModelHasContractsEnforced(
            model=model, name="check_model_has_contracts_enforced"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_EXPOSURES = [
    pytest.param(
        [
            {},  # default exposure depends on model_1
        ],
        {
            "depends_on": {"nodes": ["source.package_name.source_1"]},
        },
        does_not_raise(),
        id="has_exposure",
    ),
    pytest.param(
        [
            {
                "depends_on": {"nodes": ["model.package_name.model_2"]},
            },
        ],
        {
            "depends_on": {"nodes": ["source.package_name.source_1"]},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="no_exposure",
    ),
]


@pytest.mark.parametrize(
    ("exposures", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_EXPOSURES,
    indirect=["exposures", "model"],
)
def test_check_model_has_exposures(exposures, model, expectation):
    with expectation:
        CheckModelHasExposure(
            exposures=exposures, model=model, name="check_model_has_exposure"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_META_KEYS = [
    pytest.param(
        ["owner"],
        {
            "meta": {"owner": "Bob"},
        },
        does_not_raise(),
        id="has_key",
    ),
    pytest.param(
        ["owner"],
        {
            "meta": {"maturity": "high", "owner": "Bob"},
        },
        does_not_raise(),
        id="has_key_with_others",
    ),
    pytest.param(
        ["owner", {"name": ["first", "last"]}],
        {
            "meta": {
                "name": {"first": "Bob", "last": "Bobbington"},
                "owner": "Bob",
            },
        },
        does_not_raise(),
        id="has_nested_keys",
    ),
    pytest.param(
        ["key_1", "key_2"],
        {
            "meta": {"key_1": "abc", "key_2": ["a", "b", "c"]},
        },
        does_not_raise(),
        id="has_multiple_keys",
    ),
    pytest.param(
        ["owner"],
        {
            "meta": {},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_key",
    ),
    pytest.param(
        ["owner"],
        {
            "meta": {"maturity": "high"},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_key_with_others",
    ),
    pytest.param(
        ["owner", {"name": ["first", "last"]}],
        {
            "meta": {"name": {"last": "Bobbington"}, "owner": "Bob"},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_nested_key",
    ),
]


@pytest.mark.parametrize(
    ("keys", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_META_KEYS,
    indirect=["model"],
)
def test_check_model_has_meta_keys(keys, model, expectation):
    with expectation:
        CheckModelHasMetaKeys(
            keys=keys, model=model, name="check_model_has_meta_keys"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_NO_UPSTREAM_DEPENDENCIES = [
    pytest.param(
        {
            "depends_on": {"nodes": ["source.package_name.source_1"]},
        },
        does_not_raise(),
        id="depends_on_source",
    ),
    pytest.param(
        {
            "alias": "int_model_1",
            "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
            "fqn": ["package_name", "int_model_1"],
            "name": "int_model_1",
            "original_file_path": "models/int_model_1.sql",
            "path": "int_model_1.sql",
            "unique_id": "model.package_name.int_model_1",
        },
        does_not_raise(),
        id="depends_on_model",
    ),
    pytest.param(
        {
            "depends_on": {"nodes": []},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="no_dependencies",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_NO_UPSTREAM_DEPENDENCIES,
    indirect=["model"],
)
def test_check_model_has_no_upstream_dependencies(model, expectation):
    with expectation:
        CheckModelHasNoUpstreamDependencies(
            model=model, name="check_model_has_no_upstream_dependencies"
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


_TEST_DATA_FOR_CHECK_MODEL_HAS_TAGS = [
    pytest.param(
        {
            "tags": ["tag_1"],
        },
        ["tag_1"],
        "all",
        does_not_raise(),
        id="has_all_tags",
    ),
    pytest.param(
        {
            "tags": [],
        },
        ["tag_1"],
        "all",
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_tag",
    ),
    pytest.param(
        {
            "tags": ["tag_1", "tag_2"],
        },
        ["tag_1", "tag_2"],
        "all",
        does_not_raise(),
        id="has_all_multiple_tags",
    ),
    pytest.param(
        {
            "tags": ["tag_1"],
        },
        ["tag_1", "tag_2"],
        "all",
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_one_tag",
    ),
    pytest.param(
        {
            "tags": ["tag_1"],
        },
        ["tag_1", "tag_2"],
        "any",
        does_not_raise(),
        id="has_any_tag",
    ),
    pytest.param(
        {
            "tags": ["tag_3", "tag_4"],
        },
        ["tag_1", "tag_2"],
        "any",
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_any_tag",
    ),
    pytest.param(
        {
            "tags": ["tag_1", "tag_3"],
        },
        ["tag_1", "tag_2"],
        "one",
        does_not_raise(),
        id="has_one_tag",
    ),
    pytest.param(
        {
            "tags": ["tag_1", "tag_2"],
        },
        ["tag_1", "tag_2"],
        "one",
        pytest.raises(DbtBouncerFailedCheckError),
        id="has_more_than_one_tag",
    ),
]


@pytest.mark.parametrize(
    ("model", "tags", "criteria", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_TAGS,
    indirect=["model"],
)
def test_check_model_has_tags(model, tags, criteria, expectation):
    with expectation:
        CheckModelHasTags(
            model=model,
            name="check_model_has_tags",
            tags=tags,
            criteria=criteria,
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_UNIQUE_TEST = [
    pytest.param(
        ["expect_compound_columns_to_be_unique", "unique"],
        {},
        [{}],
        does_not_raise(),
        id="has_unique_test",
    ),
    pytest.param(
        ["my_custom_test", "unique"],
        {},
        [
            {"test_metadata": {"name": "my_custom_test"}},
        ],
        does_not_raise(),
        id="has_custom_unique_test",
    ),
    pytest.param(
        ["unique"],
        {},
        [
            {"test_metadata": {"name": "expect_compound_columns_to_be_unique"}},
        ],
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_unique_test_strict",
    ),
    pytest.param(
        ["expect_compound_columns_to_be_unique", "unique"],
        {},
        [],
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_unique_test",
    ),
]


@pytest.mark.parametrize(
    ("accepted_uniqueness_tests", "model", "tests", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_UNIQUE_TEST,
    indirect=["model", "tests"],
)
def test_check_model_has_unique_test(
    accepted_uniqueness_tests,
    model,
    tests,
    expectation,
):
    with expectation:
        CheckModelHasUniqueTest(
            accepted_uniqueness_tests=accepted_uniqueness_tests,
            model=model,
            name="check_model_has_unique_test",
            tests=tests,
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_HAS_UNIT_TESTS = [
    pytest.param(
        "manifest_obj",
        1,
        {},
        [{}],
        does_not_raise(),
        id="has_unit_test",
    ),
    pytest.param(
        "manifest_obj",
        2,
        {},
        [{}],
        pytest.raises(DbtBouncerFailedCheckError),
        id="not_enough_unit_tests",
    ),
]


@pytest.mark.parametrize(
    ("manifest_obj", "min_number_of_unit_tests", "model", "unit_tests", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_HAS_UNIT_TESTS,
    indirect=["manifest_obj", "model", "unit_tests"],
)
def test_check_model_has_unit_tests(
    manifest_obj,
    min_number_of_unit_tests,
    model,
    unit_tests,
    expectation,
):
    with expectation:
        CheckModelHasUnitTests(
            manifest_obj=manifest_obj,
            min_number_of_unit_tests=min_number_of_unit_tests,
            model=model,
            name="check_model_has_unit_tests",
            unit_tests=unit_tests,
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_LATEST_VERSION_SPECIFIED = [
    pytest.param(
        {
            "latest_version": 2,
        },
        does_not_raise(),
        id="latest_version_integer",
    ),
    pytest.param(
        {
            "latest_version": "stable",
        },
        does_not_raise(),
        id="latest_version_string",
    ),
    pytest.param(
        {
            "latest_version": None,
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_latest_version",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_LATEST_VERSION_SPECIFIED,
    indirect=["model"],
)
def test_check_model_latest_version_specified(
    model,
    expectation,
):
    with expectation:
        CheckModelLatestVersionSpecified(
            model=model,
            name="check_model_latest_version_specified",
        ).execute()


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


_TEST_DATA_FOR_CHECK_MODEL_DIRECTORIES = [
    pytest.param(
        "models",
        {
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
        },
        ["staging", "mart", "intermediate"],
        does_not_raise(),
        id="valid_directory",
    ),
    pytest.param(
        "models/marts",
        {
            "original_file_path": "models/marts/finance/marts_model_1.sql",
            "path": "marts/finance/marts_model_1.sql",
        },
        ["finance", "marketing"],
        does_not_raise(),
        id="valid_subdirectory",
    ),
    pytest.param(
        "models/marts/",
        {
            "original_file_path": "models/marts/finance/marts_model_1.sql",
            "path": "marts/finance/marts_model_1.sql",
        },
        ["finance", "marketing"],
        does_not_raise(),
        id="valid_subdirectory_trailing_slash",
    ),
    pytest.param(
        "models/marts",
        {
            "original_file_path": "models/marts/sales/marts_model_1.sql",
            "path": "marts/sales/marts_model_1.sql",
        },
        ["finance", "marketing"],
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_subdirectory",
    ),
    pytest.param(
        "models",
        {
            "original_file_path": "models/model_1.sql",
            "path": "marts/sales/model_1.sql",
        },
        ["finance", "marketing"],
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_root_directory",
    ),
]


@pytest.mark.parametrize(
    ("include", "model", "permitted_sub_directories", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DIRECTORIES,
    indirect=["model"],
)
def test_check_model_directories(
    include,
    model,
    permitted_sub_directories,
    expectation,
):
    with expectation:
        CheckModelDirectories(
            include=include,
            model=model,
            name="check_model_directories",
            permitted_sub_directories=permitted_sub_directories,
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_MAX_CHAINED_VIEWS = [
    pytest.param(
        "manifest_obj",
        ["ephemeral", "view"],
        3,
        {
            "alias": "model_0",
            "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
            "fqn": ["dbt_bouncer_test_project", "model_1"],
            "name": "model_0",
            "original_file_path": "models/marts/sales/model_0.sql",
            "package_name": "dbt_bouncer_test_project",
            "path": "marts/sales/model_0.sql",
            "unique_id": "model.dbt_bouncer_test_project.model_0",
        },
        [
            {
                "alias": "model_0",
                "config": {"materialized": "ephemeral"},
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.model_1"],
                },
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_0",
                "original_file_path": "models/marts/sales/model_0.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_0.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_0",
            },
            {
                "alias": "model_1",
                "config": {"materialized": "ephemeral"},
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.model_2"],
                },
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_1",
                "original_file_path": "models/marts/sales/model_1.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_1.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_1",
            },
            {
                "alias": "model_2",
                "config": {"materialized": "view"},
                "depends_on": {"nodes": []},
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_2",
                "original_file_path": "models/marts/sales/model_2.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_2.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_2",
            },
        ],
        does_not_raise(),
        id="within_limit",
    ),
    pytest.param(
        "manifest_obj",
        ["ephemeral", "view"],
        3,
        {
            "alias": "model_0",
            "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
            "fqn": ["dbt_bouncer_test_project", "model_1"],
            "name": "model_0",
            "original_file_path": "models/marts/sales/model_0.sql",
            "package_name": "dbt_bouncer_test_project",
            "path": "marts/sales/model_0.sql",
            "unique_id": "model.dbt_bouncer_test_project.model_0",
        },
        [
            {
                "alias": "model_0",
                "config": {"materialized": "ephemeral"},
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.model_1"],
                },
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_0",
                "original_file_path": "models/marts/sales/model_0.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_0.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_0",
            },
            {
                "alias": "model_1",
                "config": {"materialized": "ephemeral"},
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.model_2"],
                },
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_1",
                "original_file_path": "models/marts/sales/model_1.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_1.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_1",
            },
            {
                "alias": "model_2",
                "config": {"materialized": "view"},
                "depends_on": {
                    "nodes": ["model.dbt_bouncer_test_project.model_3"],
                },
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_2",
                "original_file_path": "models/marts/sales/model_2.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_2.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_2",
            },
            {
                "alias": "model_3",
                "config": {"materialized": "view"},
                "depends_on": {"nodes": []},
                "fqn": ["dbt_bouncer_test_project", "model_1"],
                "name": "model_3",
                "original_file_path": "models/marts/sales/model_3.sql",
                "package_name": "dbt_bouncer_test_project",
                "path": "marts/sales/model_3.sql",
                "unique_id": "model.dbt_bouncer_test_project.model_3",
            },
        ],
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_limit",
    ),
]


@pytest.mark.parametrize(
    (
        "manifest_obj",
        "materializations_to_include",
        "max_chained_views",
        "model",
        "models",
        "expectation",
    ),
    _TEST_DATA_FOR_CHECK_MODEL_MAX_CHAINED_VIEWS,
    indirect=["manifest_obj", "model", "models"],
)
def test_check_model_max_chained_views(
    manifest_obj,
    materializations_to_include,
    max_chained_views,
    model,
    models,
    expectation,
):
    with expectation:
        CheckModelMaxChainedViews(
            manifest_obj=manifest_obj,
            materializations_to_include=materializations_to_include,
            max_chained_views=max_chained_views,
            model=model,
            models=models,
            name="check_model_max_chained_views",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_NAMES = [
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
        does_not_raise(),
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
        does_not_raise(),
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
        does_not_raise(),
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
        does_not_raise(),
        id="valid_name_int",
    ),
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
        pytest.raises(DbtBouncerFailedCheckError),
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
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_name_int_suffix",
    ),
]


@pytest.mark.parametrize(
    ("include", "model_name_pattern", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_NAMES,
    indirect=["model"],
)
def test_check_mode_names(include, model_name_pattern, model, expectation):
    with expectation:
        CheckModelNames(
            include=include,
            model_name_pattern=model_name_pattern,
            model=model,
            name="check_model_names",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_NUMBER_OF_GRANTS = [
    pytest.param(
        1,
        1,
        {
            "config": {"grants": {"select": ["user1"]}},
        },
        does_not_raise(),
        id="within_limits",
    ),
    pytest.param(
        1,
        1,
        {
            "config": {"grants": {"select": ["user1"], "write": ["user1"]}},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_max",
    ),
    pytest.param(
        2,
        2,
        {
            "config": {"grants": {"select": ["user1"]}},
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="below_min",
    ),
]


@pytest.mark.parametrize(
    ("max_number_of_privileges", "min_number_of_privileges", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_NUMBER_OF_GRANTS,
    indirect=["model"],
)
def test_check_model_number_of_grants(
    max_number_of_privileges, min_number_of_privileges, model, expectation
):
    with expectation:
        CheckModelNumberOfGrants(
            max_number_of_privileges=max_number_of_privileges,
            min_number_of_privileges=min_number_of_privileges,
            model=model,
            name="check_model_number_of_grants",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_MAX_FANOUT = [
    pytest.param(
        1,
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        [
            {
                "alias": "stg_model_2",
                "depends_on": {
                    "nodes": [
                        "model.package_name.stg_model_1",
                    ],
                },
                "fqn": ["package_name", "stg_model_2"],
                "name": "stg_model_2",
                "original_file_path": "models/staging/stg_model_2.sql",
                "path": "staging/stg_model_2.sql",
                "unique_id": "model.package_name.stg_model_2",
            },
        ],
        does_not_raise(),
        id="within_fanout_limit",
    ),
    pytest.param(
        1,
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        [
            {
                "alias": "stg_model_2",
                "depends_on": {
                    "nodes": [
                        "model.package_name.stg_model_1",
                    ],
                },
                "fqn": ["package_name", "stg_model_2"],
                "name": "stg_model_2",
                "original_file_path": "models/staging/stg_model_2.sql",
                "path": "staging/stg_model_2.sql",
                "unique_id": "model.package_name.stg_model_2",
            },
            {
                "alias": "stg_model_3",
                "depends_on": {
                    "nodes": [
                        "model.package_name.stg_model_1",
                    ],
                },
                "fqn": ["package_name", "stg_model_3"],
                "name": "stg_model_3",
                "original_file_path": "models/staging/stg_model_3.sql",
                "path": "staging/stg_model_3.sql",
                "unique_id": "model.package_name.stg_model_3",
            },
        ],
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_fanout_limit",
    ),
]


@pytest.mark.parametrize(
    ("max_downstream_models", "model", "models", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_MAX_FANOUT,
    indirect=["model", "models"],
)
def test_check_model_max_fanout(max_downstream_models, model, models, expectation):
    with expectation:
        CheckModelMaxFanout(
            max_downstream_models=max_downstream_models,
            model=model,
            models=models,
            name="check_model_max_fanout",
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


_TEST_DATA_FOR_CHECK_MODEL_MAX_UPSTREAM_DEPENDENCIES = [
    pytest.param(
        5,
        5,
        1,
        {
            "depends_on": {
                "macros": [
                    "macro.package_name.macro_1",
                    "macro.package_name.macro_2",
                    "macro.package_name.macro_3",
                    "macro.package_name.macro_4",
                    "macro.package_name.macro_5",
                ],
                "nodes": [
                    "model.package_name.stg_model_1",
                    "model.package_name.stg_model_2",
                    "model.package_name.stg_model_3",
                    "model.package_name.stg_model_4",
                    "model.package_name.stg_model_5",
                    "source.package_name.source_1",
                ],
            },
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        does_not_raise(),
        id="within_limits",
    ),
    pytest.param(
        5,
        5,
        1,
        {
            "depends_on": {
                "macros": [],
                "nodes": [],
            },
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        does_not_raise(),
        id="no_dependencies",
    ),
    pytest.param(
        5,
        5,
        1,
        {
            "depends_on": {
                "macros": [],
                "nodes": [
                    "source.package_name.source_1",
                    "source.package_name.source_2",
                ],
            },
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_source_limit",
    ),
    pytest.param(
        5,
        5,
        1,
        {
            "depends_on": {
                "macros": [
                    "macro.package_name.macro_1",
                    "macro.package_name.macro_2",
                    "macro.package_name.macro_3",
                    "macro.package_name.macro_4",
                    "macro.package_name.macro_5",
                    "macro.package_name.macro_6",
                ],
                "nodes": [],
            },
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_macro_limit",
    ),
    pytest.param(
        5,
        5,
        1,
        {
            "depends_on": {
                "macros": [],
                "nodes": [
                    "model.package_name.stg_model_1",
                    "model.package_name.stg_model_2",
                    "model.package_name.stg_model_3",
                    "model.package_name.stg_model_4",
                    "model.package_name.stg_model_5",
                    "model.package_name.stg_model_6",
                ],
            },
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.stg_model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="exceeds_model_limit",
    ),
]


@pytest.mark.parametrize(
    (
        "max_upstream_macros",
        "max_upstream_models",
        "max_upstream_sources",
        "model",
        "expectation",
    ),
    _TEST_DATA_FOR_CHECK_MODEL_MAX_UPSTREAM_DEPENDENCIES,
    indirect=["model"],
)
def test_check_model_max_upstream_dependencies(
    max_upstream_macros,
    max_upstream_models,
    max_upstream_sources,
    model,
    expectation,
):
    with expectation:
        CheckModelMaxUpstreamDependencies(
            max_upstream_macros=max_upstream_macros,
            max_upstream_models=max_upstream_models,
            max_upstream_sources=max_upstream_sources,
            model=model,
            name="check_model_max_upstream_dependencies",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_PROPERTY_FILE_LOCATION = [
    pytest.param(
        {
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        does_not_raise(),
        id="valid_location_stg",
    ),
    pytest.param(
        {
            "original_file_path": "models/intermediate/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_int_crm__models.yml",
            "path": "intermediate/crm/stg_model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        does_not_raise(),
        id="valid_location_int",
    ),
    pytest.param(
        {
            "original_file_path": "models/marts/crm/stg_model_1.sql",
            "patch_path": "package_name://models/marts/crm/_crm__models.yml",
            "path": "marts/crm/stg_model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        does_not_raise(),
        id="valid_location_marts",
    ),
    pytest.param(
        {
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_staging_crm__models.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
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
        pytest.raises(DbtBouncerFailedCheckError),
        id="missing_underscore",
    ),
    pytest.param(
        {
            "original_file_path": "models/staging/crm/stg_model_1.sql",
            "patch_path": "package_name://models/staging/crm/_schema.yml",
            "path": "staging/crm/stg_model_1.sql",
            "unique_id": "model.package_name.model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_name",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_PROPERTY_FILE_LOCATION,
    indirect=["model"],
)
def test_check_model_property_file_location(model, expectation):
    with expectation:
        CheckModelPropertyFileLocation(
            model=model, name="check_model_property_file_location"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_SCHEMA_NAME = [
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
        does_not_raise(),
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
        does_not_raise(),
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
        does_not_raise(),
        id="valid_schema_ignored_dir",
    ),
    pytest.param(
        "^intermediate",
        ".*intermediate",
        {
            "alias": "model_1",
            "fqn": ["package_name", "model_1"],
            "name": "model_1",
            "original_file_path": "models/intermediate/model_1.sql",
            "path": "intermediate/model_1.sql",
            "schema": "dbt_jdoe_int_domain",
            "unique_id": "model.package_name.model_1",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="invalid_schema_int",
    ),
]


@pytest.mark.parametrize(
    ("include", "schema_name_pattern", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_SCHEMA_NAME,
    indirect=["model"],
)
def test_check_model_schema_name(include, schema_name_pattern, model, expectation):
    with expectation:
        CheckModelSchemaName(
            include=include,
            schema_name_pattern=schema_name_pattern,
            model=model,
            name="check_model_schema_name",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_VERSION_ALLOWED = [
    pytest.param(
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": 1,
        },
        r"[0-9]\d*",
        does_not_raise(),
        id="allowed_version_1",
    ),
    pytest.param(
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": 10,
        },
        r"[0-9]\d*",
        does_not_raise(),
        id="allowed_version_10",
    ),
    pytest.param(
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": 100,
        },
        r"[0-9]\d*",
        does_not_raise(),
        id="allowed_version_100",
    ),
    pytest.param(
        {
            "alias": "stg_model_1",
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "path": "staging/stg_model_1.sql",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": "golden",
        },
        r"[0-9]\d*",
        pytest.raises(DbtBouncerFailedCheckError),
        id="disallowed_version",
    ),
]


@pytest.mark.parametrize(
    ("model", "version_pattern", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_VERSION_ALLOWED,
    indirect=["model"],
)
def test_check_model_version_allowed(model, version_pattern, expectation):
    with expectation:
        CheckModelVersionAllowed(
            model=model,
            name="check_model_version_allowed",
            version_pattern=version_pattern,
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_VERSION_PINNED_IN_REF = [
    pytest.param(
        {
            "manifest": {
                "child_map": {
                    "model.package_name.stg_model_1": ["model.package_name.stg_model_2"]
                },
                "nodes": {
                    "model.package_name.stg_model_1": {
                        "alias": "stg_model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "stg_model_1"],
                        "name": "stg_model_1",
                        "original_file_path": "models/staging/stg_model_1.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_1.sql",
                        "resource_type": "model",
                        "schema": "dbt_jdoe_stg_domain",
                        "unique_id": "model.package_name.stg_model_1",
                        "version": 1,
                    },
                    "model.package_name.stg_model_2": {
                        "alias": "stg_model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "stg_model_2"],
                        "name": "stg_model_2",
                        "original_file_path": "models/staging/stg_model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_2.sql",
                        "refs": [{"name": "stg_model_1", "version": 1}],
                        "resource_type": "model",
                        "schema": "dbt_jdoe_stg_domain",
                        "unique_id": "model.package_name.stg_model_2",
                        "version": 1,
                    },
                },
            }
        },
        {
            "alias": "stg_model_1",
            "checksum": {"name": "sha256", "checksum": ""},
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
            },
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "package_name": "package_name",
            "path": "staging/stg_model_1.sql",
            "resource_type": "model",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": 1,
        },
        does_not_raise(),
        id="pinned_version",
    ),
    pytest.param(
        {
            "manifest": {
                "child_map": {
                    "model.package_name.stg_model_1": ["model.package_name.stg_model_2"]
                },
                "nodes": {
                    "model.package_name.stg_model_1": {
                        "alias": "stg_model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "stg_model_1"],
                        "name": "stg_model_1",
                        "original_file_path": "models/staging/stg_model_1.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_1.sql",
                        "resource_type": "model",
                        "schema": "dbt_jdoe_stg_domain",
                        "unique_id": "model.package_name.stg_model_1",
                        "version": 1,
                    },
                    "model.package_name.stg_model_2": {
                        "alias": "stg_model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "fqn": ["package_name", "stg_model_2"],
                        "name": "stg_model_2",
                        "original_file_path": "models/staging/stg_model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_2.sql",
                        "refs": [{"name": "stg_model_1", "version": None}],
                        "resource_type": "model",
                        "schema": "dbt_jdoe_stg_domain",
                        "unique_id": "model.package_name.stg_model_2",
                        "version": 1,
                    },
                },
            }
        },
        {
            "alias": "stg_model_1",
            "checksum": {"name": "sha256", "checksum": ""},
            "columns": {
                "col_1": {
                    "index": 1,
                    "name": "col_1",
                    "type": "INTEGER",
                },
            },
            "fqn": ["package_name", "stg_model_1"],
            "name": "stg_model_1",
            "original_file_path": "models/staging/stg_model_1.sql",
            "package_name": "package_name",
            "path": "staging/stg_model_1.sql",
            "resource_type": "model",
            "schema": "dbt_jdoe_stg_domain",
            "unique_id": "model.package_name.stg_model_1",
            "version": 1,
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="unpinned_version",
    ),
]


@pytest.mark.parametrize(
    ("manifest_obj", "model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_VERSION_PINNED_IN_REF,
    indirect=["manifest_obj", "model"],
)
def test_check_model_version_pinned_in_ref(manifest_obj, model, expectation):
    with expectation:
        CheckModelVersionPinnedInRef(
            manifest_obj=manifest_obj,
            model=model,
            name="check_model_version_pinned_in_ref",
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_DESCRIPTION_CONTAINS_REGEX_PATTERN = [
    pytest.param(
        {
            "description": "Description that contains the pattern to match.",
        },
        ".*pattern to match.*",
        does_not_raise(),
        id="contains_pattern_single_line",
    ),
    pytest.param(
        {
            "description": """A
                        multiline
                        description
                        with the pattern to match.
                        """,
        },
        ".*pattern to match.*",
        does_not_raise(),
        id="contains_pattern_multiline",
    ),
    pytest.param(
        {
            "description": "",
        },
        ".*pattern to match.*",
        pytest.raises(DbtBouncerFailedCheckError),
        id="empty_description",
    ),
    pytest.param(
        {
            "description": " ",
        },
        ".*pattern to match.*",
        pytest.raises(DbtBouncerFailedCheckError),
        id="whitespace_description",
    ),
    pytest.param(
        {
            "description": """
                        """,
        },
        ".*pattern to match.*",
        pytest.raises(DbtBouncerFailedCheckError),
        id="multiline_whitespace_description",
    ),
    pytest.param(
        {
            "description": "Description with a pattern that does not match.",
        },
        ".*pattern to match.*",
        pytest.raises(DbtBouncerFailedCheckError),
        id="does_not_contain_pattern",
    ),
    pytest.param(
        {
            "description": """Description with
                    the
                    pattern to match.""",
        },
        ".*pattern to match.*",
        does_not_raise(),
        id="contains_pattern_multiline_split",
    ),
]


@pytest.mark.parametrize(
    ("model", "pattern", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DESCRIPTION_CONTAINS_REGEX_PATTERN,
    indirect=["model"],
)
def test_check_model_description_contains_regex_pattern(model, pattern, expectation):
    with expectation:
        CheckModelDescriptionContainsRegexPattern(
            model=model,
            name="check_model_description_contains_regex_pattern",
            regexp_pattern=pattern,
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_DESCRIPTION_POPULATED = [
    pytest.param(
        {
            "description": "Description that is more than 4 characters.",
        },
        does_not_raise(),
        id="populated_description",
    ),
    pytest.param(
        {
            "description": """A
                        multiline
                        description
                        """,
        },
        does_not_raise(),
        id="multiline_description",
    ),
    pytest.param(
        {
            "description": "",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="empty_description",
    ),
    pytest.param(
        {
            "description": " ",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="whitespace_description",
    ),
    pytest.param(
        {
            "description": """
                        """,
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="multiline_whitespace_description",
    ),
    pytest.param(
        {
            "description": "-",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="too_short_description",
    ),
    pytest.param(
        {
            "description": "null",
        },
        pytest.raises(DbtBouncerFailedCheckError),
        id="null_description",
    ),
]


@pytest.mark.parametrize(
    ("model", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_DESCRIPTION_POPULATED,
    indirect=["model"],
)
def test_check_model_description_populated(model, expectation):
    with expectation:
        CheckModelDescriptionPopulated(
            model=model, name="check_model_description_populated"
        ).execute()


_TEST_DATA_FOR_CHECK_MODEL_TEST_COVERAGE = [
    pytest.param(
        100,
        [{}],
        [
            {
                "alias": "not_null_model_1_unique",
                "attached_node": "model.package_name.model_1",
                "checksum": {"name": "none", "checksum": ""},
                "column_name": "col_1",
                "depends_on": {
                    "nodes": [
                        "model.package_name.model_1",
                    ],
                },
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
                    "name": "expect_compound_columns_to_be_unique",
                },
                "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
            },
        ],
        does_not_raise(),
        id="100_percent_coverage",
    ),
    pytest.param(
        50,
        [
            {
                "alias": "model_1",
                "fqn": ["package_name", "model_1"],
                "name": "model_1",
                "original_file_path": "model_1.sql",
                "path": "staging/finance/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            {
                "alias": "model_2",
                "fqn": ["package_name", "model_2"],
                "name": "model_2",
                "original_file_path": "model_2.sql",
                "path": "staging/finance/model_2.sql",
                "unique_id": "model.package_name.model_2",
            },
        ],
        [
            {
                "alias": "not_null_model_1_unique",
                "attached_node": "model.package_name.model_1",
                "depends_on": {
                    "nodes": [
                        "model.package_name.model_1",
                    ],
                },
                "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
            },
        ],
        does_not_raise(),
        id="50_percent_coverage",
    ),
    pytest.param(
        100,
        [
            {
                "alias": "model_1",
                "fqn": ["package_name", "model_1"],
                "name": "model_1",
                "original_file_path": "model_1.sql",
                "path": "staging/finance/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            {
                "alias": "model_2",
                "fqn": ["package_name", "model_2"],
                "name": "model_2",
                "original_file_path": "model_2.sql",
                "path": "staging/finance/model_2.sql",
                "unique_id": "model.package_name.model_2",
            },
        ],
        [
            {
                "alias": "not_null_model_1_unique",
                "attached_node": "model.package_name.model_1",
                "depends_on": {
                    "nodes": [
                        "model.package_name.model_2",
                    ],
                },
                "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
            },
        ],
        pytest.raises(DbtBouncerFailedCheckError),
        id="less_than_100_percent_coverage",
    ),
]


@pytest.mark.parametrize(
    ("min_model_test_coverage_pct", "models", "tests", "expectation"),
    _TEST_DATA_FOR_CHECK_MODEL_TEST_COVERAGE,
    indirect=["models", "tests"],
)
def test_check_model_test_coverage(
    min_model_test_coverage_pct,
    models,
    tests,
    expectation,
):
    with expectation:
        CheckModelsTestCoverage(
            min_model_test_coverage_pct=min_model_test_coverage_pct,
            models=models,
            name="check_model_test_coverage",
            tests=tests,
        ).execute()
