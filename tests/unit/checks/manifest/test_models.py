from contextlib import nullcontext as does_not_raise

import pytest

from dbt_bouncer.checks.manifest.check_models import (
    check_model_access,
    check_model_code_does_not_contain_regexp_pattern,
    check_model_contract_enforced_for_public_model,
    check_model_depends_on_multiple_sources,
    check_model_description_populated,
    check_model_directories,
    check_model_documentation_coverage,
    check_model_documented_in_same_directory,
    check_model_has_meta_keys,
    check_model_has_no_upstream_dependencies,
    check_model_has_unique_test,
    check_model_max_chained_views,
    check_model_max_fanout,
    check_model_max_upstream_dependencies,
    check_model_names,
    check_model_test_coverage,
)


@pytest.mark.parametrize(
    "check_config, model, expectation",
    [
        (
            {
                "access": "public",
            },
            {
                "access": "public",
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {"access": "public"},
            {
                "access": "protected",
                "unique_id": "model.package_name.mart_model_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_access(check_config, model, expectation):
    with expectation:
        check_model_access(check_config=check_config, model=model, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            {
                "access": "public",
                "contract": {"enforced": True},
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "access": "protected",
                "contract": {"enforced": False},
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "access": "public",
                "contract": {"enforced": False},
                "unique_id": "model.package_name.mart_model_1",
            },
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
            {
                "depends_on": {"nodes": ["source.package_name.source_1"]},
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "depends_on": {
                    "nodes": ["source.package_name.source_1", "source.package_name.source_2"]
                },
                "unique_id": "model.package_name.stg_model_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_depends_on_multiple_sources(model, expectation):
    with expectation:
        check_model_depends_on_multiple_sources(model=model, request=None)


@pytest.mark.parametrize(
    "check_config, models, expectation",
    [
        (
            {
                "min_model_documentation_coverage_pct": 100,
            },
            [
                {
                    "description": "Model 1 description",
                    "unique_id": "model.package_name.model_1",
                }
            ],
            does_not_raise(),
        ),
        (
            {
                "min_model_documentation_coverage_pct": 50,
            },
            [
                {
                    "description": "Model 1 description",
                    "unique_id": "model.package_name.model_1",
                },
                {
                    "description": "",
                    "unique_id": "model.package_name.model_2",
                },
            ],
            does_not_raise(),
        ),
        (
            {
                "min_model_documentation_coverage_pct": 100,
            },
            [
                {
                    "description": "",
                    "unique_id": "model.package_name.model_1",
                },
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_documentation_coverage(check_config, models, expectation):
    with expectation:
        check_model_documentation_coverage(check_config=check_config, models=models, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            {
                "patch_path": "package_name://models/staging/_schema.yml",
                "path": "staging/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "patch_path": "package_name://models/staging/_schema.yml",
                "path": "staging/finance/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_documented_in_same_directory(model, expectation):
    with expectation:
        check_model_documented_in_same_directory(model=model, request=None)


@pytest.mark.parametrize(
    "check_config, model, expectation",
    [
        (
            {"keys": ["owner"]},
            {
                "meta": {"owner": "Bob"},
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {"keys": ["owner"]},
            {
                "meta": {"maturity": "high", "owner": "Bob"},
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {"keys": ["owner", {"name": ["first", "last"]}]},
            {
                "meta": {"name": {"first": "Bob", "last": "Bobbington"}, "owner": "Bob"},
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {"keys": ["owner"]},
            {
                "meta": {},
                "unique_id": "model.package_name.model_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {"keys": ["owner"]},
            {
                "meta": {"maturity": "high"},
                "unique_id": "model.package_name.model_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {"keys": ["owner", {"name": ["first", "last"]}]},
            {
                "meta": {"name": {"last": "Bobbington"}, "owner": "Bob"},
                "unique_id": "model.package_name.model_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_has_meta_keys(check_config, model, expectation):
    with expectation:
        check_model_has_meta_keys(check_config=check_config, model=model, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            {
                "depends_on": {"nodes": ["source.package_name.source_1"]},
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "depends_on": {"nodes": ["model.package_name.stg_model_1"]},
                "unique_id": "model.package_name.int_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "depends_on": {"nodes": []},
                "unique_id": "model.package_name.model_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_has_no_upstream_dependencies(model, expectation):
    with expectation:
        check_model_has_no_upstream_dependencies(model=model, request=None)


@pytest.mark.parametrize(
    "check_config, model, tests, expectation",
    [
        (
            {
                "accepted_uniqueness_tests": ["expect_compound_columns_to_be_unique", "unique"],
            },
            {
                "unique_id": "model.package_name.stg_model_1",
            },
            [
                {
                    "attached_node": "model.package_name.stg_model_1",
                    "test_metadata": {"name": "unique"},
                }
            ],
            does_not_raise(),
        ),
        (
            {
                "accepted_uniqueness_tests": ["my_custom_test", "unique"],
            },
            {
                "unique_id": "model.package_name.stg_model_2",
            },
            [
                {
                    "attached_node": "model.package_name.stg_model_2",
                    "test_metadata": {"name": "my_custom_test"},
                }
            ],
            does_not_raise(),
        ),
        (
            {
                "accepted_uniqueness_tests": ["unique"],
            },
            {
                "unique_id": "model.package_name.stg_model_3",
            },
            [
                {
                    "attached_node": "model.package_name.stg_model_3",
                    "test_metadata": {"name": "expect_compound_columns_to_be_unique"},
                }
            ],
            pytest.raises(AssertionError),
        ),
        (
            {
                "accepted_uniqueness_tests": ["expect_compound_columns_to_be_unique", "unique"],
            },
            {
                "unique_id": "model.package_name.stg_model_4",
            },
            [],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_has_unique_test(check_config, model, tests, expectation):
    with expectation:
        check_model_has_unique_test(
            check_config=check_config, model=model, tests=tests, request=None
        )


@pytest.mark.parametrize(
    "check_config, model, expectation",
    [
        (
            {"regexp_pattern": ".*[i][f][n][u][l][l].*"},
            {
                "raw_code": "select coalesce(a, b) from table",
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "regexp_pattern": ".*[i][f][n][u][l][l].*",
            },
            {
                "raw_code": "select ifnull(a, b) from table",
                "unique_id": "model.package_name.stg_model_2",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_code_does_not_contain_regexp_pattern(check_config, model, expectation):
    with expectation:
        check_model_code_does_not_contain_regexp_pattern(
            check_config=check_config, model=model, request=None
        )


@pytest.mark.parametrize(
    "check_config, model, expectation",
    [
        (
            {"include": "", "permitted_sub_directories": ["staging", "mart", "intermediate"]},
            {
                "path": "staging/stg_model_1.sql",
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {"include": "marts", "permitted_sub_directories": ["finance", "marketing"]},
            {
                "path": "marts/finance/marts_model_1.sql",
                "unique_id": "model.package_name.marts_model_1",
            },
            does_not_raise(),
        ),
        (
            {"include": "marts", "permitted_sub_directories": ["finance", "marketing"]},
            {
                "path": "marts/sales/marts_model_1.sql",
                "unique_id": "model.package_name.marts_model_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_directories(check_config, model, expectation):
    with expectation:
        check_model_directories(check_config=check_config, model=model, request=None)


@pytest.mark.parametrize(
    "check_config, manifest_obj, model, models, expectation",
    [
        (
            {"materializations_to_include": ["ephemeral", "view"], "max_chained_views": 3},
            "manifest_obj",
            {
                "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                "unique_id": "model.dbt_bouncer_test_project.model_0",
            },
            [
                {
                    "config": {"materialized": "ephemeral"},
                    "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                    "unique_id": "model.dbt_bouncer_test_project.model_0",
                },
                {
                    "config": {"materialized": "ephemeral"},
                    "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_2"]},
                    "unique_id": "model.dbt_bouncer_test_project.model_1",
                },
                {
                    "config": {"materialized": "view"},
                    "depends_on": {"nodes": []},
                    "unique_id": "model.dbt_bouncer_test_project.model_2",
                },
            ],
            does_not_raise(),
        ),
        (
            {"materializations_to_include": ["ephemeral", "view"], "max_chained_views": 3},
            "manifest_obj",
            {
                "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                "unique_id": "model.dbt_bouncer_test_project.model_0",
            },
            [
                {
                    "config": {"materialized": "ephemeral"},
                    "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_1"]},
                    "unique_id": "model.dbt_bouncer_test_project.model_0",
                },
                {
                    "config": {"materialized": "ephemeral"},
                    "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_2"]},
                    "unique_id": "model.dbt_bouncer_test_project.model_1",
                },
                {
                    "config": {"materialized": "view"},
                    "depends_on": {"nodes": ["model.dbt_bouncer_test_project.model_3"]},
                    "unique_id": "model.dbt_bouncer_test_project.model_2",
                },
                {
                    "config": {"materialized": "view"},
                    "depends_on": {"nodes": []},
                    "unique_id": "model.dbt_bouncer_test_project.model_3",
                },
            ],
            pytest.raises(AssertionError),
        ),
    ],
    indirect=["manifest_obj"],
)
def test_check_model_max_chained_views(check_config, manifest_obj, model, models, expectation):
    with expectation:
        check_model_max_chained_views(
            check_config=check_config,
            manifest_obj=manifest_obj,
            model=model,
            models=models,
            request=None,
        )


@pytest.mark.parametrize(
    "check_config, model, expectation",
    [
        (
            {
                "model_name_pattern": "^stg_",
            },
            {
                "name": "stg_model_1",
                "path": "staging/stg_model_1.sql",
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "include": "^staging",
                "model_name_pattern": "^stg_",
            },
            {
                "name": "stg_model_2",
                "path": "staging/stg_model_2.sql",
                "unique_id": "model.package_name.stg_model_2",
            },
            does_not_raise(),
        ),
        (
            {
                "include": "^intermediate",
                "model_name_pattern": "^stg_",
            },
            {
                "name": "stg_model_3",
                "path": "staging/stg_model_3.sql",
                "unique_id": "model.package_name.stg_model_3",
            },
            does_not_raise(),
        ),
        (
            {
                "include": "^intermediate",
                "model_name_pattern": "^int_",
            },
            {
                "name": "int_model_1",
                "path": "intermediate/int_model_1.sql",
                "unique_id": "model.package_name.int_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "include": "^intermediate",
                "model_name_pattern": "^int_",
            },
            {
                "name": "model_1",
                "path": "intermediate/model_1.sql",
                "unique_id": "model.package_name.model_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "include": "^intermediate",
                "model_name_pattern": "^int_",
            },
            {
                "name": "model_int_2",
                "path": "intermediate/model_int_2.sql",
                "unique_id": "model.package_name.model_int_2",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_mode_names(check_config, model, expectation):
    with expectation:
        check_model_names(check_config=check_config, model=model, request=None)


@pytest.mark.parametrize(
    "check_config, model, models, expectation",
    [
        (
            {
                "max_downstream_models": 1,
            },
            {
                "unique_id": "model.package_name.stg_model_1",
            },
            [
                {
                    "depends_on": {
                        "nodes": [
                            "model.package_name.stg_model_1",
                        ]
                    },
                    "unique_id": "model.package_name.stg_model_2",
                },
            ],
            does_not_raise(),
        ),
        (
            {
                "max_downstream_models": 1,
            },
            {
                "unique_id": "model.package_name.stg_model_1",
            },
            [
                {
                    "depends_on": {
                        "nodes": [
                            "model.package_name.stg_model_1",
                        ]
                    },
                    "unique_id": "model.package_name.stg_model_2",
                },
                {
                    "depends_on": {
                        "nodes": [
                            "model.package_name.stg_model_1",
                        ]
                    },
                    "unique_id": "model.package_name.stg_model_3",
                },
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_max_fanout(check_config, model, models, expectation):
    with expectation:
        check_model_max_fanout(check_config=check_config, model=model, models=models, request=None)


@pytest.mark.parametrize(
    "check_config, model, expectation",
    [
        (
            {
                "max_upstream_macros": 5,
                "max_upstream_models": 5,
                "max_upstream_sources": 1,
            },
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
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "max_upstream_macros": 5,
                "max_upstream_models": 5,
                "max_upstream_sources": 1,
            },
            {
                "depends_on": {
                    "macros": [],
                    "nodes": [],
                },
                "unique_id": "model.package_name.stg_model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "max_upstream_macros": 5,
                "max_upstream_models": 5,
                "max_upstream_sources": 1,
            },
            {
                "depends_on": {
                    "macros": [],
                    "nodes": [
                        "source.package_name.source_1",
                        "source.package_name.source_2",
                    ],
                },
                "unique_id": "model.package_name.stg_model_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "max_upstream_macros": 5,
                "max_upstream_models": 5,
                "max_upstream_sources": 1,
            },
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
                "unique_id": "model.package_name.stg_model_1",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "max_upstream_macros": 5,
                "max_upstream_models": 5,
                "max_upstream_sources": 1,
            },
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
                "unique_id": "model.package_name.stg_model_1",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_max_upstream_dependencies(check_config, model, expectation):
    with expectation:
        check_model_max_upstream_dependencies(check_config=check_config, model=model, request=None)


@pytest.mark.parametrize(
    "model, expectation",
    [
        (
            {
                "description": "Description that is more than 4 characters.",
                "unique_id": "model.package_name.model_1",
            },
            does_not_raise(),
        ),
        (
            {
                "description": """A
                        multiline
                        description
                        """,
                "unique_id": "model.package_name.model_2",
            },
            does_not_raise(),
        ),
        (
            {
                "description": "",
                "unique_id": "model.package_name.model_3",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": " ",
                "unique_id": "model.package_name.model_4",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": """
                        """,
                "unique_id": "model.package_name.model_5",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": "-",
                "unique_id": "model.package_name.model_6",
            },
            pytest.raises(AssertionError),
        ),
        (
            {
                "description": "null",
                "unique_id": "model.package_name.model_7",
            },
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_description_populated(model, expectation):
    with expectation:
        check_model_description_populated(model=model, request=None)


@pytest.mark.parametrize(
    "check_config, models, tests, expectation",
    [
        (
            {
                "min_model_test_coverage_pct": 100,
            },
            [
                {
                    "unique_id": "model.package_name.model_1",
                }
            ],
            [
                {
                    "depends_on": {
                        "nodes": [
                            "model.package_name.model_1",
                        ],
                    }
                }
            ],
            does_not_raise(),
        ),
        (
            {
                "min_model_test_coverage_pct": 50,
            },
            [
                {
                    "unique_id": "model.package_name.model_1",
                },
                {
                    "unique_id": "model.package_name.model_2",
                },
            ],
            [
                {
                    "depends_on": {
                        "nodes": [
                            "model.package_name.model_1",
                        ],
                    }
                }
            ],
            does_not_raise(),
        ),
        (
            {
                "min_model_test_coverage_pct": 100,
            },
            [
                {
                    "unique_id": "model.package_name.model_1",
                },
                {
                    "unique_id": "model.package_name.model_2",
                },
            ],
            [
                {
                    "depends_on": {
                        "nodes": [
                            "model.package_name.model_2",
                        ],
                    }
                }
            ],
            pytest.raises(AssertionError),
        ),
    ],
)
def test_check_model_test_coverage(check_config, models, tests, expectation):
    with expectation:
        check_model_test_coverage(
            check_config=check_config, models=models, tests=tests, request=None
        )
