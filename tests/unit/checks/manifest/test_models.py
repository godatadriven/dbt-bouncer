import warnings
from contextlib import nullcontext as does_not_raise

import pytest

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=UserWarning)
    from dbt_artifacts_parser.parsers.manifest.manifest_v12 import Nodes4, Nodes6, UnitTests

from dbt_bouncer.checks.manifest.check_models import (
    check_model_access,
    check_model_code_does_not_contain_regexp_pattern,
    check_model_contract_enforced_for_public_model,
    check_model_depends_on_multiple_sources,
    check_model_description_populated,
    check_model_directories,
    check_model_documentation_coverage,
    check_model_documented_in_same_directory,
    check_model_has_contracts_enforced,
    check_model_has_meta_keys,
    check_model_has_no_upstream_dependencies,
    check_model_has_tags,
    check_model_has_unique_test,
    check_model_has_unit_tests,
    check_model_max_chained_views,
    check_model_max_fanout,
    check_model_max_upstream_dependencies,
    check_model_names,
    check_model_property_file_location,
    check_model_test_coverage,
)


@pytest.mark.parametrize(
    "access, model, expectation",
    [
        (
            "public",
            Nodes4(
                **{
                    "access": "public",
                    "alias": "model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
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
                    "package_name": "package_name",
                    "path": "model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_2",
                }
            ),
            does_not_raise(),
        ),
        (
            "public",
            Nodes4(
                **{
                    "access": "protected",
                    "alias": "model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
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
                    "package_name": "package_name",
                    "path": "model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_2",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_access(access, model, expectation):
    with expectation:
        check_model_access(access=access, model=model, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            Nodes4(
                **{
                    "access": "public",
                    "alias": "model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "contract": {"enforced": True},
                    "fqn": ["package_name", "model_2"],
                    "name": "model_2",
                    "original_file_path": "model_2.sql",
                    "package_name": "package_name",
                    "path": "model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_2",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
                    "access": "protected",
                    "alias": "model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "contract": {"enforced": False},
                    "fqn": ["package_name", "model_2"],
                    "name": "model_2",
                    "original_file_path": "model_2.sql",
                    "package_name": "package_name",
                    "path": "model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_2",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
                    "access": "public",
                    "alias": "model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "contract": {"enforced": False},
                    "fqn": ["package_name", "model_2"],
                    "name": "model_2",
                    "original_file_path": "model_2.sql",
                    "package_name": "package_name",
                    "path": "model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_2",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_contract_enforced_for_public_model(model, expectation):
    with expectation:
        check_model_contract_enforced_for_public_model(model=model, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            Nodes4(
                **{
                    "alias": "model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                    "fqn": ["package_name", "model_2"],
                    "name": "model_2",
                    "original_file_path": "model_2.sql",
                    "package_name": "package_name",
                    "path": "model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_2",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
                    "alias": "model_2",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {
                        "nodes": ["source.package_name.source_1", "source.package_name.source_2"]
                    },
                    "fqn": ["package_name", "model_2"],
                    "name": "model_2",
                    "original_file_path": "model_2.sql",
                    "package_name": "package_name",
                    "path": "model_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_2",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_depends_on_multiple_sources(model, expectation):
    with expectation:
        check_model_depends_on_multiple_sources(model=model, request=None)


@pytest.mark.parametrize(
    "min_model_documentation_coverage_pct, models, expectation",
    [
        (
            100,
            [
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "description": "Model 2 description",
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "model_2.sql",
                        "package_name": "package_name",
                        "path": "model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                ),
            ],
            does_not_raise(),
        ),
        (
            50,
            [
                Nodes4(
                    **{
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "description": "Model 1 description",
                        "fqn": ["package_name", "model_1"],
                        "name": "model_1",
                        "original_file_path": "model_1.sql",
                        "package_name": "package_name",
                        "path": "model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_1",
                    }
                ),
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "description": "",
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "model_2.sql",
                        "package_name": "package_name",
                        "path": "model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                ),
            ],
            does_not_raise(),
        ),
        (
            100,
            [
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "description": "",
                        "fqn": ["package_name", "model_2"],
                        "name": "model_2",
                        "original_file_path": "model_2.sql",
                        "package_name": "package_name",
                        "path": "model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_documentation_coverage(
    min_model_documentation_coverage_pct, models, expectation
):
    with expectation:
        check_model_documentation_coverage(
            min_model_documentation_coverage_pct=min_model_documentation_coverage_pct,
            models=models,
            request=None,
        )


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            Nodes4(
                **{
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
                    "patch_path": "package_name://models/staging/_schema.yml",
                    "path": "staging/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
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
                    "patch_path": "package_name://models/staging/_schema.yml",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_documented_in_same_directory(model, expectation):
    with expectation:
        check_model_documented_in_same_directory(model=model, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "contract": {"enforced": True},
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "contract": {"enforced": False},
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_has_contracts_enforced(model, expectation):
    with expectation:
        check_model_has_contracts_enforced(model=model, request=None)


@pytest.mark.parametrize(
    "keys, model, expectation",
    [
        (
            ["owner"],
            Nodes4(
                **{
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
                    "meta": {"owner": "Bob"},
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            ["owner"],
            Nodes4(
                **{
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
                    "meta": {"maturity": "high", "owner": "Bob"},
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            ["owner", {"name": ["first", "last"]}],
            Nodes4(
                **{
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
                    "meta": {"name": {"first": "Bob", "last": "Bobbington"}, "owner": "Bob"},
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            ["owner"],
            Nodes4(
                **{
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
                    "meta": {},
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            ["owner"],
            Nodes4(
                **{
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
                    "meta": {"maturity": "high"},
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            ["owner", {"name": ["first", "last"]}],
            Nodes4(
                **{
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
                    "meta": {"name": {"last": "Bobbington"}, "owner": "Bob"},
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_has_meta_keys(keys, model, expectation):
    with expectation:
        check_model_has_meta_keys(keys=keys, model=model, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["source.package_name.source_1"]},
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
                    "alias": "int_model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
                    "fqn": ["package_name", "int_model_1"],
                    "name": "int_model_1",
                    "original_file_path": "models/int_model_1.sql",
                    "package_name": "package_name",
                    "path": "int_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.int_model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": []},
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_has_no_upstream_dependencies(model, expectation):
    with expectation:
        check_model_has_no_upstream_dependencies(model=model, request=None)


@pytest.mark.parametrize(
    "model, tags, expectation",
    [
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": []},
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "tags": ["tag_1"],
                    "unique_id": "model.package_name.model_1",
                }
            ),
            ["tag_1"],
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": []},
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "tags": [],
                    "unique_id": "model.package_name.model_1",
                }
            ),
            ["tag_1"],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_has_tags(model, tags, expectation):
    with expectation:
        check_model_has_tags(model=model, tags=tags, request=None)


@pytest.mark.parametrize(
    "accepted_uniqueness_tests, model, tests, expectation",
    [
        (
            ["expect_compound_columns_to_be_unique", "unique"],
            Nodes4(
                **{
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
            ),
            [
                Nodes6(
                    **{
                        "alias": "not_null_model_1_unique",
                        "attached_node": "model.package_name.model_1",
                        "checksum": {"name": "none", "checksum": ""},
                        "column_name": "col_1",
                        "fqn": ["package_name", "marts", "finance", "not_null_model_1_unique"],
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
                )
            ],
            does_not_raise(),
        ),
        (
            ["my_custom_test", "unique"],
            Nodes4(
                **{
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
            ),
            [
                Nodes6(
                    **{
                        "alias": "not_null_model_1_unique",
                        "attached_node": "model.package_name.model_1",
                        "checksum": {"name": "none", "checksum": ""},
                        "column_name": "col_1",
                        "fqn": ["package_name", "marts", "finance", "not_null_model_1_unique"],
                        "name": "not_null_model_1_unique",
                        "original_file_path": "models/marts/finance/_finance__models.yml",
                        "package_name": "package_name",
                        "path": "not_null_model_1_unique.sql",
                        "resource_type": "test",
                        "schema": "main",
                        "test_metadata": {
                            "name": "my_custom_test",
                        },
                        "unique_id": "test.package_name.not_null_model_1_unique.cf6c17daed",
                    }
                )
            ],
            does_not_raise(),
        ),
        (
            ["unique"],
            Nodes4(
                **{
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
            ),
            [
                Nodes6(
                    **{
                        "alias": "not_null_model_1_unique",
                        "attached_node": "model.package_name.model_1",
                        "checksum": {"name": "none", "checksum": ""},
                        "column_name": "col_1",
                        "fqn": ["package_name", "marts", "finance", "not_null_model_1_unique"],
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
                    }
                )
            ],
            pytest.raises(AssertionError),
        ),
        (
            ["expect_compound_columns_to_be_unique", "unique"],
            Nodes4(
                **{
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
            ),
            [],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_has_unique_test(accepted_uniqueness_tests, model, tests, expectation):
    with expectation:
        check_model_has_unique_test(
            accepted_uniqueness_tests=accepted_uniqueness_tests,
            model=model,
            tests=tests,
            request=None,
        )


@pytest.mark.parametrize(
    "manifest_obj, minimum_number_of_unit_tests, model, unit_tests, expectation",
    [
        (
            "manifest_obj",
            1,
            Nodes4(
                **{
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
            ),
            [
                UnitTests(
                    **{
                        "depends_on": {
                            "nodes": [
                                "model.package_name.model_1",
                            ]
                        },
                        "expect": {"format": "dict", "rows": [{"id": 1}]},
                        "fqn": ["package_name", "staging", "crm", "model_1", "unit_test_1"],
                        "given": [{"input": "ref(input_1)", "format": "csv"}],
                        "model": "model_1",
                        "name": "unit_test_1",
                        "original_file_path": "models/staging/crm/_crm__source.yml",
                        "resource_type": "unit_test",
                        "package_name": "package_name",
                        "path": "staging/crm/_crm__source.yml",
                        "unique_id": "unit_test.package_name.model_1.unit_test_1",
                    }
                ),
            ],
            does_not_raise(),
        ),
        (
            "manifest_obj",
            2,
            Nodes4(
                **{
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
            ),
            [
                UnitTests(
                    **{
                        "depends_on": {
                            "nodes": [
                                "model.package_name.model_1",
                            ]
                        },
                        "expect": {"format": "dict", "rows": [{"id": 1}]},
                        "fqn": ["package_name", "staging", "crm", "model_1", "unit_test_1"],
                        "given": [{"input": "ref(input_1)", "format": "csv"}],
                        "model": "model_1",
                        "name": "unit_test_1",
                        "original_file_path": "models/staging/crm/_crm__source.yml",
                        "resource_type": "unit_test",
                        "package_name": "package_name",
                        "path": "staging/crm/_crm__source.yml",
                        "unique_id": "unit_test.package_name.model_1.unit_test_1",
                    }
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_model_has_unit_tests(
    manifest_obj, minimum_number_of_unit_tests, model, unit_tests, expectation
):
    with expectation:
        check_model_has_unit_tests(
            manifest_obj=manifest_obj,
            minimum_number_of_unit_tests=minimum_number_of_unit_tests,
            model=model,
            unit_tests=unit_tests,
            request=None,
        )


@pytest.mark.parametrize(
    "model, regexp_pattern, expectation",
    [
        (
            Nodes4(
                **{
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
                    "raw_code": "select coalesce(a, b) from table",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            ".*[i][f][n][u][l][l].*",
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
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
                    "raw_code": "select ifnull(a, b) from table",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            ".*[i][f][n][u][l][l].*",
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_code_does_not_contain_regexp_pattern(model, regexp_pattern, expectation):
    with expectation:
        check_model_code_does_not_contain_regexp_pattern(
            model=model, regexp_pattern=regexp_pattern, request=None
        )


@pytest.mark.parametrize(
    "include, model, permitted_sub_directories, expectation",
    [
        (
            "",
            Nodes4(
                **{
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
                    "original_file_path": "models/staging/stg_model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/stg_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            ["staging", "mart", "intermediate"],
            does_not_raise(),
        ),
        (
            "marts",
            Nodes4(
                **{
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
                    "original_file_path": "models/marts/finance/marts_model_1.sql",
                    "package_name": "package_name",
                    "path": "marts/finance/marts_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            ["finance", "marketing"],
            does_not_raise(),
        ),
        (
            "marts",
            Nodes4(
                **{
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
                    "original_file_path": "models/marts/sales/marts_model_1.sql",
                    "package_name": "package_name",
                    "path": "marts/sales/marts_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            ["finance", "marketing"],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_directories(include, model, permitted_sub_directories, expectation):
    with expectation:
        check_model_directories(
            include=include,
            model=model,
            permitted_sub_directories=permitted_sub_directories,
            request=None,
        )


@pytest.mark.parametrize(
    "manifest_obj, materializations_to_include, max_chained_views, model, models, expectation",
    [
        (
            "manifest_obj",
            ["ephemeral", "view"],
            3,
            Nodes4(
                **{
                    "alias": "model_0",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                    "fqn": ["dbt_bouncer_test_project", "model_1"],
                    "name": "model_0",
                    "original_file_path": "models/marts/sales/model_0.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "marts/sales/model_0.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.dbt_bouncer_test_project.model_0",
                }
            ),
            [
                Nodes4(
                    **{
                        "alias": "model_0",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "config": {"materialized": "ephemeral"},
                        "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                        "fqn": ["dbt_bouncer_test_project", "model_1"],
                        "name": "model_0",
                        "original_file_path": "models/marts/sales/model_0.sql",
                        "package_name": "dbt_bouncer_test_project",
                        "path": "marts/sales/model_0.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.dbt_bouncer_test_project.model_0",
                    }
                ),
                Nodes4(
                    **{
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "config": {"materialized": "ephemeral"},
                        "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_2"]},
                        "fqn": ["dbt_bouncer_test_project", "model_1"],
                        "name": "model_1",
                        "original_file_path": "models/marts/sales/model_1.sql",
                        "package_name": "dbt_bouncer_test_project",
                        "path": "marts/sales/model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.dbt_bouncer_test_project.model_1",
                    }
                ),
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "config": {"materialized": "view"},
                        "depends_on": {"nodes": []},
                        "fqn": ["dbt_bouncer_test_project", "model_1"],
                        "name": "model_2",
                        "original_file_path": "models/marts/sales/model_2.sql",
                        "package_name": "dbt_bouncer_test_project",
                        "path": "marts/sales/model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.dbt_bouncer_test_project.model_2",
                    }
                ),
            ],
            does_not_raise(),
        ),
        (
            "manifest_obj",
            ["ephemeral", "view"],
            3,
            Nodes4(
                **{
                    "alias": "model_0",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                    "fqn": ["dbt_bouncer_test_project", "model_1"],
                    "name": "model_0",
                    "original_file_path": "models/marts/sales/model_0.sql",
                    "package_name": "dbt_bouncer_test_project",
                    "path": "marts/sales/model_0.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.dbt_bouncer_test_project.model_0",
                }
            ),
            [
                Nodes4(
                    **{
                        "alias": "model_0",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "config": {"materialized": "ephemeral"},
                        "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                        "fqn": ["dbt_bouncer_test_project", "model_1"],
                        "name": "model_0",
                        "original_file_path": "models/marts/sales/model_0.sql",
                        "package_name": "dbt_bouncer_test_project",
                        "path": "marts/sales/model_0.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.dbt_bouncer_test_project.model_0",
                    }
                ),
                Nodes4(
                    **{
                        "alias": "model_1",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "config": {"materialized": "ephemeral"},
                        "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_2"]},
                        "fqn": ["dbt_bouncer_test_project", "model_1"],
                        "name": "model_1",
                        "original_file_path": "models/marts/sales/model_1.sql",
                        "package_name": "dbt_bouncer_test_project",
                        "path": "marts/sales/model_1.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.dbt_bouncer_test_project.model_1",
                    }
                ),
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "config": {"materialized": "view"},
                        "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_3"]},
                        "fqn": ["dbt_bouncer_test_project", "model_1"],
                        "name": "model_2",
                        "original_file_path": "models/marts/sales/model_2.sql",
                        "package_name": "dbt_bouncer_test_project",
                        "path": "marts/sales/model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.dbt_bouncer_test_project.model_2",
                    }
                ),
                Nodes4(
                    **{
                        "alias": "model_3",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "config": {"materialized": "view"},
                        "depends_on": {"nodes": []},
                        "fqn": ["dbt_bouncer_test_project", "model_1"],
                        "name": "model_3",
                        "original_file_path": "models/marts/sales/model_3.sql",
                        "package_name": "dbt_bouncer_test_project",
                        "path": "marts/sales/model_3.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.dbt_bouncer_test_project.model_3",
                    }
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_model_max_chained_views(
    manifest_obj, materializations_to_include, max_chained_views, model, models, expectation
):
    with expectation:
        check_model_max_chained_views(
            manifest_obj=manifest_obj,
            materializations_to_include=materializations_to_include,
            max_chained_views=max_chained_views,
            model=model,
            models=models,
            request=None,
        )


@pytest.mark.parametrize(
    "include, model_name_pattern, model, expectation",
    [
        (
            "",
            "^stg_",
            Nodes4(
                **{
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
                    "schema": "main",
                    "unique_id": "model.package_name.stg_model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            "^staging",
            "^stg_",
            Nodes4(
                **{
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
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.stg_model_2",
                }
            ),
            does_not_raise(),
        ),
        (
            "^intermediate",
            "^stg_",
            Nodes4(
                **{
                    "alias": "stg_model_3",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "fqn": ["package_name", "stg_model_3"],
                    "name": "stg_model_3",
                    "original_file_path": "models/staging/stg_model_3.sql",
                    "package_name": "package_name",
                    "path": "staging/stg_model_3.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.stg_model_3",
                }
            ),
            does_not_raise(),
        ),
        (
            "^intermediate",
            "^int_",
            Nodes4(
                **{
                    "alias": "int_model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "fqn": ["package_name", "int_model_1"],
                    "name": "int_model_1",
                    "original_file_path": "models/intermediate/int_model_1.sql",
                    "package_name": "package_name",
                    "path": "intermediate/int_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.int_model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            "^intermediate",
            "^int_",
            Nodes4(
                **{
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
                    "original_file_path": "models/intermediate/model_1.sql",
                    "package_name": "package_name",
                    "path": "intermediate/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            "^intermediate",
            "^int_",
            Nodes4(
                **{
                    "alias": "model_int_2",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "fqn": ["package_name", "model_int_2"],
                    "name": "model_int_2",
                    "original_file_path": "models/intermediate/model_int_2.sql",
                    "package_name": "package_name",
                    "path": "intermediate/model_int_2.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_int_2",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_mode_names(include, model_name_pattern, model, expectation):
    with expectation:
        check_model_names(
            include=include, model_name_pattern=model_name_pattern, model=model, request=None
        )


@pytest.mark.parametrize(
    "max_downstream_models, model, models, expectation",
    [
        (
            1,
            Nodes4(
                **{
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
                    "schema": "main",
                    "unique_id": "model.package_name.stg_model_1",
                }
            ),
            [
                Nodes4(
                    **{
                        "alias": "stg_model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {
                            "nodes": [
                                "model.package_name.stg_model_1",
                            ]
                        },
                        "fqn": ["package_name", "stg_model_2"],
                        "name": "stg_model_2",
                        "original_file_path": "models/staging/stg_model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.stg_model_2",
                    }
                ),
            ],
            does_not_raise(),
        ),
        (
            1,
            Nodes4(
                **{
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
                    "schema": "main",
                    "unique_id": "model.package_name.stg_model_1",
                }
            ),
            [
                Nodes4(
                    **{
                        "alias": "stg_model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {
                            "nodes": [
                                "model.package_name.stg_model_1",
                            ]
                        },
                        "fqn": ["package_name", "stg_model_2"],
                        "name": "stg_model_2",
                        "original_file_path": "models/staging/stg_model_2.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.stg_model_2",
                    }
                ),
                Nodes4(
                    **{
                        "alias": "stg_model_3",
                        "checksum": {"name": "sha256", "checksum": ""},
                        "columns": {
                            "col_1": {
                                "index": 1,
                                "name": "col_1",
                                "type": "INTEGER",
                            },
                        },
                        "depends_on": {
                            "nodes": [
                                "model.package_name.stg_model_1",
                            ]
                        },
                        "fqn": ["package_name", "stg_model_3"],
                        "name": "stg_model_3",
                        "original_file_path": "models/staging/stg_model_3.sql",
                        "package_name": "package_name",
                        "path": "staging/stg_model_3.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.stg_model_3",
                    }
                ),
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_max_fanout(max_downstream_models, model, models, expectation):
    with expectation:
        check_model_max_fanout(
            max_downstream_models=max_downstream_models, model=model, models=models, request=None
        )


@pytest.mark.parametrize(
    "max_upstream_macros, max_upstream_models, max_upstream_sources, model, expectation",
    [
        (
            5,
            5,
            1,
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
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
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.stg_model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            5,
            5,
            1,
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {
                        "macros": [],
                        "nodes": [],
                    },
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.stg_model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            5,
            5,
            1,
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "depends_on": {
                        "macros": [],
                        "nodes": [
                            "source.package_name.source_1",
                            "source.package_name.source_2",
                        ],
                    },
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.stg_model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            5,
            5,
            1,
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
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
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.stg_model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            5,
            5,
            1,
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
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
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.stg_model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_max_upstream_dependencies(
    max_upstream_macros, max_upstream_models, max_upstream_sources, model, expectation
):
    with expectation:
        check_model_max_upstream_dependencies(
            max_upstream_macros=max_upstream_macros,
            max_upstream_models=max_upstream_models,
            max_upstream_sources=max_upstream_sources,
            model=model,
            request=None,
        )


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            Nodes4(
                **{
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
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://models/staging/crm/_stg_crm__models.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
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
                    "original_file_path": "models/intermediate/crm/stg_model_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://models/staging/crm/_int_crm__models.yml",
                    "path": "intermediate/crm/stg_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
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
                    "original_file_path": "models/marts/crm/stg_model_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://models/marts/crm/_crm__models.yml",
                    "path": "marts/crm/stg_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
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
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://models/staging/crm/_staging_crm__models.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Nodes4(
                **{
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
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "patch_path": "package_name://models/staging/crm/_models.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "package_name": "package_name",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Nodes4(
                **{
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
                    "original_file_path": "models/staging/crm/stg_model_1.sql",
                    "package_name": "package_name",
                    "patch_path": "package_name://models/staging/crm/_schema.yml",
                    "path": "staging/crm/stg_model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_property_file_location(model, expectation):
    with expectation:
        check_model_property_file_location(model=model, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "description": "Description that is more than 4 characters.",
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "description": """A
                        multiline
                        description
                        """,
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            does_not_raise(),
        ),
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "description": "",
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "description": " ",
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "description": """
                        """,
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "description": "-",
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
        (
            Nodes4(
                **{
                    "alias": "model_1",
                    "checksum": {"name": "sha256", "checksum": ""},
                    "columns": {
                        "col_1": {
                            "index": 1,
                            "name": "col_1",
                            "type": "INTEGER",
                        },
                    },
                    "description": "null",
                    "fqn": ["package_name", "model_1"],
                    "name": "model_1",
                    "original_file_path": "model_1.sql",
                    "package_name": "package_name",
                    "path": "staging/finance/model_1.sql",
                    "resource_type": "model",
                    "schema": "main",
                    "unique_id": "model.package_name.model_1",
                }
            ),
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_description_populated(model, expectation):
    with expectation:
        check_model_description_populated(model=model, request=None)


@pytest.mark.parametrize(
    "min_model_test_coverage_pct, models, tests, expectation",
    [
        (
            100,
            [
                Nodes4(
                    **{
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
                ),
            ],
            [
                Nodes6(
                    **{
                        "alias": "not_null_model_1_unique",
                        "attached_node": "model.package_name.model_1",
                        "checksum": {"name": "none", "checksum": ""},
                        "column_name": "col_1",
                        "depends_on": {
                            "nodes": [
                                "model.package_name.model_1",
                            ],
                        },
                        "fqn": ["package_name", "marts", "finance", "not_null_model_1_unique"],
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
                    }
                )
            ],
            does_not_raise(),
        ),
        (
            50,
            [
                Nodes4(
                    **{
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
                ),
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
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
                        "package_name": "package_name",
                        "path": "staging/finance/model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                ),
            ],
            [
                Nodes6(
                    **{
                        "alias": "not_null_model_1_unique",
                        "attached_node": "model.package_name.model_1",
                        "checksum": {"name": "none", "checksum": ""},
                        "column_name": "col_1",
                        "depends_on": {
                            "nodes": [
                                "model.package_name.model_1",
                            ],
                        },
                        "fqn": ["package_name", "marts", "finance", "not_null_model_1_unique"],
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
                    }
                )
            ],
            does_not_raise(),
        ),
        (
            100,
            [
                Nodes4(
                    **{
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
                ),
                Nodes4(
                    **{
                        "alias": "model_2",
                        "checksum": {"name": "sha256", "checksum": ""},
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
                        "package_name": "package_name",
                        "path": "staging/finance/model_2.sql",
                        "resource_type": "model",
                        "schema": "main",
                        "unique_id": "model.package_name.model_2",
                    }
                ),
            ],
            [
                Nodes6(
                    **{
                        "alias": "not_null_model_1_unique",
                        "attached_node": "model.package_name.model_1",
                        "checksum": {"name": "none", "checksum": ""},
                        "column_name": "col_1",
                        "depends_on": {
                            "nodes": [
                                "model.package_name.model_2",
                            ],
                        },
                        "fqn": ["package_name", "marts", "finance", "not_null_model_1_unique"],
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
                    }
                )
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_test_coverage(min_model_test_coverage_pct, models, tests, expectation):
    with expectation:
        check_model_test_coverage(
            min_model_test_coverage_pct=min_model_test_coverage_pct,
            models=models,
            tests=tests,
            request=None,
        )
