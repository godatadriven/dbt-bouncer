import pytest

from dbt_bouncer.testing import check_fails, check_passes


class TestCheckModelColumnsHaveRelationshipTests:
    @pytest.mark.parametrize(
        (
            "column_name_pattern",
            "target_column_pattern",
            "target_model_pattern",
            "model_override",
            "ctx_tests",
        ),
        [
            pytest.param(
                "_fk$",
                None,
                None,
                {
                    "columns": {
                        "user_fk": {"name": "user_fk"},
                    },
                },
                [
                    {
                        "test_metadata": {
                            "name": "relationships",
                            "kwargs": {
                                "column_name": "user_fk",
                                "field": "user_pk",
                                "to": "ref('dim_users')",
                            },
                        },
                    },
                ],
                id="column_has_relationships_test",
            ),
            pytest.param(
                "_fk$",
                "_pk$",
                None,
                {
                    "columns": {
                        "user_fk": {"name": "user_fk"},
                    },
                },
                [
                    {
                        "test_metadata": {
                            "name": "relationships",
                            "kwargs": {
                                "column_name": "user_fk",
                                "field": "user_pk",
                                "to": "ref('dim_users')",
                            },
                        },
                    },
                ],
                id="target_column_matches_pattern",
            ),
            pytest.param(
                "_fk$",
                "_pk$",
                "^dim_",
                {
                    "columns": {
                        "user_fk": {"name": "user_fk"},
                    },
                },
                [
                    {
                        "test_metadata": {
                            "name": "relationships",
                            "kwargs": {
                                "column_name": "user_fk",
                                "field": "user_pk",
                                "to": "ref('dim_users')",
                            },
                        },
                    },
                ],
                id="target_model_matches_pattern",
            ),
            pytest.param(
                "_fk$",
                None,
                None,
                {
                    "columns": {
                        "user_id": {"name": "user_id"},
                    },
                },
                [],
                id="no_columns_match_pattern",
            ),
            pytest.param(
                "_fk$",
                None,
                None,
                {"columns": {}},
                [],
                id="no_columns",
            ),
        ],
    )
    def test_pass(
        self,
        column_name_pattern,
        target_column_pattern,
        target_model_pattern,
        model_override,
        ctx_tests,
    ):
        check_passes(
            "check_model_columns_have_relationship_tests",
            column_name_pattern=column_name_pattern,
            target_column_pattern=target_column_pattern,
            target_model_pattern=target_model_pattern,
            model=model_override,
            ctx_tests=ctx_tests,
        )

    @pytest.mark.parametrize(
        (
            "column_name_pattern",
            "target_column_pattern",
            "target_model_pattern",
            "model_override",
            "ctx_tests",
        ),
        [
            pytest.param(
                "_fk$",
                None,
                None,
                {
                    "columns": {
                        "user_fk": {"name": "user_fk"},
                    },
                },
                [],
                id="no_relationships_test",
            ),
            pytest.param(
                "_fk$",
                None,
                None,
                {
                    "columns": {
                        "user_fk": {"name": "user_fk"},
                    },
                },
                [
                    {
                        "test_metadata": {
                            "name": "not_null",
                            "kwargs": {
                                "column_name": "user_fk",
                            },
                        },
                    },
                ],
                id="has_test_but_not_relationships",
            ),
            pytest.param(
                "_fk$",
                "_pk$",
                None,
                {
                    "columns": {
                        "user_fk": {"name": "user_fk"},
                    },
                },
                [
                    {
                        "test_metadata": {
                            "name": "relationships",
                            "kwargs": {
                                "column_name": "user_fk",
                                "field": "user_id",
                                "to": "ref('dim_users')",
                            },
                        },
                    },
                ],
                id="target_column_does_not_match_pattern",
            ),
            pytest.param(
                "_fk$",
                None,
                "^dim_",
                {
                    "columns": {
                        "user_fk": {"name": "user_fk"},
                    },
                },
                [
                    {
                        "test_metadata": {
                            "name": "relationships",
                            "kwargs": {
                                "column_name": "user_fk",
                                "field": "user_pk",
                                "to": "ref('stg_users')",
                            },
                        },
                    },
                ],
                id="target_model_does_not_match_pattern",
            ),
        ],
    )
    def test_fail(
        self,
        column_name_pattern,
        target_column_pattern,
        target_model_pattern,
        model_override,
        ctx_tests,
    ):
        check_fails(
            "check_model_columns_have_relationship_tests",
            column_name_pattern=column_name_pattern,
            target_column_pattern=target_column_pattern,
            target_model_pattern=target_model_pattern,
            model=model_override,
            ctx_tests=ctx_tests,
        )


class TestCheckModelColumnsHaveMetaKeys:
    @pytest.mark.parametrize(
        ("keys", "model_override"),
        [
            pytest.param(
                ["owner"],
                {
                    "columns": {
                        "col_1": {
                            "name": "col_1",
                            "meta": {"owner": "data-team"},
                        },
                    },
                },
                id="column_has_required_key",
            ),
            pytest.param(
                ["owner"],
                {"columns": {}},
                id="no_columns",
            ),
        ],
    )
    def test_pass(self, keys, model_override):
        check_passes(
            "check_model_columns_have_meta_keys", keys=keys, model=model_override
        )

    @pytest.mark.parametrize(
        ("keys", "model_override"),
        [
            pytest.param(
                ["owner"],
                {
                    "columns": {
                        "col_1": {
                            "name": "col_1",
                            "meta": {},
                        },
                    },
                },
                id="column_missing_required_key",
            ),
            pytest.param(
                ["owner"],
                {
                    "columns": {
                        "col_1": {
                            "name": "col_1",
                            "meta": {"maturity": "high"},
                        },
                    },
                },
                id="column_has_other_key_but_missing_required",
            ),
        ],
    )
    def test_fail(self, keys, model_override):
        check_fails(
            "check_model_columns_have_meta_keys", keys=keys, model=model_override
        )


class TestCheckModelColumnsHaveTypes:
    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {
                    "columns": {
                        "col_1": {"name": "col_1", "data_type": "varchar"},
                    },
                },
                id="column_has_type",
            ),
            pytest.param(
                {"columns": {}},
                id="no_columns",
            ),
        ],
    )
    def test_pass(self, model_override):
        check_passes("check_model_columns_have_types", model=model_override)

    @pytest.mark.parametrize(
        "model_override",
        [
            pytest.param(
                {
                    "columns": {
                        "col_1": {"name": "col_1"},
                    },
                },
                id="column_missing_type",
            ),
            pytest.param(
                {
                    "columns": {
                        "col_1": {"name": "col_1", "data_type": "integer"},
                        "col_2": {"name": "col_2"},
                    },
                },
                id="one_column_missing_type",
            ),
        ],
    )
    def test_fail(self, model_override):
        check_fails("check_model_columns_have_types", model=model_override)


class TestCheckModelHasConstraints:
    @pytest.mark.parametrize(
        ("required_constraint_types", "model_override"),
        [
            pytest.param(
                ["primary_key"],
                {
                    "config": {"materialized": "table"},
                    "constraints": [{"type": "primary_key"}],
                },
                id="table_has_required_constraint",
            ),
            pytest.param(
                ["primary_key"],
                {
                    "config": {"materialized": "view"},
                    "constraints": [],
                },
                id="view_skipped",
            ),
            pytest.param(
                ["primary_key"],
                {
                    "config": {"materialized": "ephemeral"},
                    "constraints": [],
                },
                id="ephemeral_skipped",
            ),
        ],
    )
    def test_pass(self, required_constraint_types, model_override):
        check_passes(
            "check_model_has_constraints",
            required_constraint_types=required_constraint_types,
            model=model_override,
        )

    @pytest.mark.parametrize(
        ("required_constraint_types", "model_override"),
        [
            pytest.param(
                ["primary_key"],
                {
                    "config": {"materialized": "table"},
                    "constraints": [],
                },
                id="table_missing_required_constraint",
            ),
            pytest.param(
                ["primary_key", "not_null"],
                {
                    "config": {"materialized": "incremental"},
                    "constraints": [{"type": "primary_key"}],
                },
                id="incremental_missing_one_constraint",
            ),
        ],
    )
    def test_fail(self, required_constraint_types, model_override):
        check_fails(
            "check_model_has_constraints",
            required_constraint_types=required_constraint_types,
            model=model_override,
        )
