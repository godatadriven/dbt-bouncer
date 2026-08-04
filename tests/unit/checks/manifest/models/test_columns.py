from enum import Enum
from types import SimpleNamespace

import pytest

from dbt_bouncer.testing import check_fails, check_passes


class _ConstraintType(Enum):
    """Stand-in for dbt's constraint-type enum.

    The checks read `c_type.value` when the constraint type is an enum member
    rather than a plain string, which is how dbt-core supplies it.
    """

    NOT_NULL = "not_null"
    PRIMARY_KEY = "primary_key"


def _cols(*names: str) -> dict:
    """Build a `columns` dict from bare column names.

    Returns:
        dict: A `columns` mapping keyed by column name.

    """
    return {name: {"name": name} for name in names}


def _rel_test(
    column_name: str,
    *,
    field: str = "user_pk",
    to: str = "ref('dim_users')",
    name: str = "relationships",
) -> dict:
    """Build a test node carrying `relationships` test_metadata.

    Returns:
        dict: A manifest test node dict.

    """
    return {
        "test_metadata": {
            "name": name,
            "kwargs": {"column_name": column_name, "field": field, "to": to},
        },
    }


class TestCheckModelColumnsHaveRelationshipTests:
    @pytest.mark.parametrize(
        (
            "column_name_pattern",
            "target_column_pattern",
            "target_model_pattern",
            "model_override",
            "ctx_tests",
            "check_fn",
        ),
        [
            pytest.param(
                "_fk$",
                None,
                None,
                {"columns": _cols("user_fk")},
                [_rel_test("user_fk")],
                check_passes,
                id="column_has_relationships_test",
            ),
            pytest.param(
                "_fk$",
                "_pk$",
                None,
                {"columns": _cols("user_fk")},
                [_rel_test("user_fk")],
                check_passes,
                id="target_column_matches_pattern",
            ),
            pytest.param(
                "_fk$",
                "_pk$",
                "^dim_",
                {"columns": _cols("user_fk")},
                [_rel_test("user_fk")],
                check_passes,
                id="target_model_matches_pattern",
            ),
            pytest.param(
                "_fk$",
                None,
                None,
                {"columns": _cols("user_id")},
                [],
                check_passes,
                id="no_columns_match_pattern",
            ),
            pytest.param(
                "_fk$",
                None,
                None,
                {"columns": {}},
                [],
                check_passes,
                id="no_columns",
            ),
            pytest.param(
                "_fk$",
                None,
                None,
                {"columns": None},
                [],
                check_passes,
                id="columns_is_none",
            ),
            # The test list is scanned until a test for this column is found, so
            # a leading test for a different column is skipped rather than
            # aborting the search.
            pytest.param(
                "_fk$",
                None,
                None,
                {"columns": _cols("user_fk")},
                [_rel_test("other_fk"), _rel_test("user_fk")],
                check_passes,
                id="relationships_test_for_another_column_is_skipped",
            ),
            # `to` that is not a ref() is compared as a literal string.
            pytest.param(
                "_fk$",
                None,
                "^source_users$",
                {"columns": _cols("user_fk")},
                [_rel_test("user_fk", to="source_users")],
                check_passes,
                id="non_ref_target_compared_literally",
            ),
            pytest.param(
                "_fk$",
                None,
                None,
                {"columns": _cols("user_fk")},
                [],
                check_fails,
                id="no_relationships_test",
            ),
            pytest.param(
                "_fk$",
                None,
                None,
                {"columns": _cols("user_fk")},
                [_rel_test("user_fk", name="not_null")],
                check_fails,
                id="has_test_but_not_relationships",
            ),
            pytest.param(
                "_fk$",
                "_pk$",
                None,
                {"columns": _cols("user_fk")},
                [_rel_test("user_fk", field="user_id")],
                check_fails,
                id="target_column_does_not_match_pattern",
            ),
            pytest.param(
                "_fk$",
                None,
                "^dim_",
                {"columns": _cols("user_fk")},
                [_rel_test("user_fk", to="ref('stg_users')")],
                check_fails,
                id="target_model_does_not_match_pattern",
            ),
            pytest.param(
                "_fk$",
                None,
                None,
                {"columns": _cols("user_fk", "order_fk")},
                [_rel_test("user_fk")],
                check_fails,
                id="one_of_two_matching_columns_untested",
            ),
        ],
    )
    def test_check_model_columns_have_relationship_tests(
        self,
        column_name_pattern,
        target_column_pattern,
        target_model_pattern,
        model_override,
        ctx_tests,
        check_fn,
    ):
        check_fn(
            "check_model_columns_have_relationship_tests",
            column_name_pattern=column_name_pattern,
            target_column_pattern=target_column_pattern,
            target_model_pattern=target_model_pattern,
            model=model_override,
            ctx_tests=ctx_tests,
        )

    @pytest.mark.parametrize(
        ("target_column_pattern", "target_model_pattern", "check_fn"),
        [
            pytest.param(None, None, check_passes, id="no_target_patterns"),
            pytest.param("_pk$", None, check_passes, id="target_column_matches"),
            pytest.param(None, "^dim_", check_passes, id="target_model_matches"),
            pytest.param("_nope$", None, check_fails, id="target_column_mismatch"),
            pytest.param(None, "^fact_", check_fails, id="target_model_mismatch"),
        ],
    )
    def test_kwargs_read_via_attributes_when_not_a_mapping(
        self, target_column_pattern, target_model_pattern, check_fn
    ):
        # dbt supplies test_metadata.kwargs as a mapping, but the check falls
        # back to attribute access for any object that is not a dict.
        check_fn(
            "check_model_columns_have_relationship_tests",
            column_name_pattern="_fk$",
            target_column_pattern=target_column_pattern,
            target_model_pattern=target_model_pattern,
            model={"columns": _cols("order_fk")},
            ctx_tests=[
                {
                    "test_metadata": {
                        "name": "relationships",
                        "kwargs": SimpleNamespace(
                            column_name="order_fk",
                            field="order_pk",
                            to="ref('dim_orders')",
                        ),
                    },
                },
            ],
        )

    @pytest.mark.parametrize(
        "test_node",
        [
            pytest.param({"test_metadata": None}, id="test_metadata_none"),
            pytest.param({}, id="test_metadata_absent"),
            pytest.param(
                {"test_metadata": {"name": "relationships"}},
                id="relationships_test_without_kwargs",
            ),
        ],
    )
    def test_fails_when_no_usable_relationships_test_is_attached(self, test_node):
        check_fails(
            "check_model_columns_have_relationship_tests",
            column_name_pattern="_fk$",
            model={"columns": _cols("user_fk")},
            ctx_tests=[test_node],
        )

    def test_failure_message_reports_each_failing_column_and_reason(self):
        check_fails(
            "check_model_columns_have_relationship_tests",
            column_name_pattern="_fk$",
            target_column_pattern="_pk$",
            model={"columns": _cols("user_fk", "order_fk")},
            ctx_tests=[_rel_test("user_fk", field="user_id")],
            match=(
                r"has columns missing required `relationships` tests: \{"
                r"'user_fk': 'target column \"user_id\" does not match pattern \"_pk\$\"', "
                r"'order_fk': 'no relationships test found'\}"
            ),
        )


class TestCheckModelColumnsHaveMetaKeys:
    @pytest.mark.parametrize(
        ("keys", "model_override", "check_fn"),
        [
            pytest.param(
                ["owner"],
                {
                    "columns": {
                        "col_1": {"name": "col_1", "meta": {"owner": "data-team"}}
                    }
                },
                check_passes,
                id="column_has_required_key",
            ),
            pytest.param(["owner"], {"columns": {}}, check_passes, id="no_columns"),
            pytest.param(
                ["owner"], {"columns": None}, check_passes, id="columns_is_none"
            ),
            pytest.param(
                [{"owner": ["team"]}],
                {
                    "columns": {
                        "col_1": {"name": "col_1", "meta": {"owner": {"team": "data"}}}
                    }
                },
                check_passes,
                id="column_has_nested_key",
            ),
            pytest.param(
                [],
                {"columns": {"col_1": {"name": "col_1", "meta": {}}}},
                check_passes,
                id="no_required_keys_vacuously_passes",
            ),
            pytest.param(
                ["owner"],
                {"columns": {"col_1": {"name": "col_1", "meta": {}}}},
                check_fails,
                id="column_missing_required_key",
            ),
            pytest.param(
                ["owner"],
                {"columns": {"col_1": {"name": "col_1", "meta": {"maturity": "high"}}}},
                check_fails,
                id="column_has_other_key_but_missing_required",
            ),
            pytest.param(
                ["owner"],
                {"columns": {"col_1": {"name": "col_1", "meta": None}}},
                check_fails,
                id="column_meta_is_none",
            ),
            pytest.param(
                ["owner"],
                {"columns": {"col_1": {"name": "col_1"}}},
                check_fails,
                id="column_meta_absent",
            ),
            pytest.param(
                [{"owner": ["team"]}],
                {
                    "columns": {
                        "col_1": {"name": "col_1", "meta": {"owner": {"other": "x"}}}
                    }
                },
                check_fails,
                id="column_missing_nested_key",
            ),
        ],
    )
    def test_check_model_columns_have_meta_keys(self, keys, model_override, check_fn):
        check_fn("check_model_columns_have_meta_keys", keys=keys, model=model_override)

    def test_failure_message_reports_only_the_failing_columns(self):
        check_fails(
            "check_model_columns_have_meta_keys",
            keys=["owner"],
            model={
                "columns": {
                    "col_1": {"name": "col_1", "meta": {"owner": "data-team"}},
                    "col_2": {"name": "col_2", "meta": {}},
                }
            },
            match=r"has columns missing required `meta` keys: \{'col_2': \['owner'\]\}",
        )


class TestCheckModelColumnsHaveTypes:
    @pytest.mark.parametrize(
        ("model_override", "check_fn"),
        [
            pytest.param(
                {"columns": {"col_1": {"name": "col_1", "data_type": "varchar"}}},
                check_passes,
                id="column_has_type",
            ),
            pytest.param({"columns": {}}, check_passes, id="no_columns"),
            pytest.param({"columns": None}, check_passes, id="columns_is_none"),
            pytest.param(
                {"columns": {"col_1": {"name": "col_1"}}},
                check_fails,
                id="column_missing_type",
            ),
            pytest.param(
                {
                    "columns": {
                        "col_1": {"name": "col_1", "data_type": "integer"},
                        "col_2": {"name": "col_2"},
                    },
                },
                check_fails,
                id="one_column_missing_type",
            ),
            # `data_type` is checked for truthiness, so an empty string counts as
            # undeclared.
            pytest.param(
                {"columns": {"col_1": {"name": "col_1", "data_type": ""}}},
                check_fails,
                id="column_data_type_empty_string",
            ),
            pytest.param(
                {"columns": {"col_1": {"name": "col_1", "data_type": None}}},
                check_fails,
                id="column_data_type_none",
            ),
        ],
    )
    def test_check_model_columns_have_types(self, model_override, check_fn):
        check_fn("check_model_columns_have_types", model=model_override)

    def test_failure_message_lists_only_the_untyped_columns(self):
        check_fails(
            "check_model_columns_have_types",
            model={
                "columns": {
                    "col_1": {"name": "col_1", "data_type": "integer"},
                    "col_2": {"name": "col_2"},
                },
            },
            match=r"has columns without a declared `data_type`: \['col_2'\]",
        )


class TestCheckModelHasConstraints:
    @pytest.mark.parametrize(
        ("required_constraint_types", "model_override", "check_fn"),
        [
            pytest.param(
                ["primary_key"],
                {
                    "config": {"materialized": "table"},
                    "constraints": [{"type": "primary_key"}],
                },
                check_passes,
                id="table_has_required_constraint",
            ),
            pytest.param(
                ["primary_key"],
                {
                    "config": {"materialized": "incremental"},
                    "constraints": [{"type": "primary_key"}],
                },
                check_passes,
                id="incremental_has_required_constraint",
            ),
            pytest.param(
                ["primary_key"],
                {"config": {"materialized": "view"}, "constraints": []},
                check_passes,
                id="view_skipped",
            ),
            pytest.param(
                ["primary_key"],
                {"config": {"materialized": "ephemeral"}, "constraints": []},
                check_passes,
                id="ephemeral_skipped",
            ),
            # Only table and incremental models are checked, so a model whose
            # materialization cannot be determined is skipped.
            pytest.param(
                ["primary_key"],
                {"config": None, "constraints": []},
                check_passes,
                id="config_is_none_skipped",
            ),
            pytest.param(
                ["primary_key"],
                {"config": {}, "constraints": []},
                check_passes,
                id="materialized_absent_skipped",
            ),
            pytest.param(
                [],
                {"config": {"materialized": "table"}, "constraints": []},
                check_passes,
                id="no_required_constraint_types",
            ),
            pytest.param(
                ["primary_key"],
                {
                    "config": {"materialized": "table"},
                    "constraints": [{"type": "primary_key"}, {"type": "unique"}],
                },
                check_passes,
                id="extra_constraints_allowed",
            ),
            pytest.param(
                ["primary_key"],
                {"config": {"materialized": "table"}, "constraints": []},
                check_fails,
                id="table_missing_required_constraint",
            ),
            pytest.param(
                ["primary_key", "not_null"],
                {
                    "config": {"materialized": "incremental"},
                    "constraints": [{"type": "primary_key"}],
                },
                check_fails,
                id="incremental_missing_one_constraint",
            ),
            pytest.param(
                ["primary_key"],
                {"config": {"materialized": "table"}, "constraints": None},
                check_fails,
                id="constraints_is_none",
            ),
        ],
    )
    def test_check_model_has_constraints(
        self, required_constraint_types, model_override, check_fn
    ):
        check_fn(
            "check_model_has_constraints",
            required_constraint_types=required_constraint_types,
            model=model_override,
        )

    def test_enum_constraint_type_is_read_via_its_value(self):
        # dbt supplies constraint types as enum members; the check reads
        # `.value` rather than str()-ing the member (which would yield
        # "_ConstraintType.PRIMARY_KEY" and never match).
        check_passes(
            "check_model_has_constraints",
            required_constraint_types=["primary_key"],
            model={
                "config": {"materialized": "table"},
                "constraints": [SimpleNamespace(type=_ConstraintType.PRIMARY_KEY)],
            },
        )

    def test_failure_message_lists_missing_types_sorted(self):
        check_fails(
            "check_model_has_constraints",
            required_constraint_types=["primary_key", "not_null"],
            model={"config": {"materialized": "table"}, "constraints": []},
            match=r"is missing required constraint types: \['not_null', 'primary_key'\]",
        )


class TestCheckModelSinglePrimaryKey:
    @pytest.mark.parametrize(
        ("model_override", "check_fn"),
        [
            pytest.param({"columns": {}}, check_passes, id="no_columns"),
            pytest.param({"columns": None}, check_passes, id="columns_is_none"),
            pytest.param(
                {"columns": {"col_1": {"name": "col_1"}}},
                check_passes,
                id="column_without_constraints",
            ),
            pytest.param(
                {"columns": {"col_1": {"name": "col_1", "constraints": None}}},
                check_passes,
                id="column_constraints_is_none",
            ),
            pytest.param(
                {
                    "columns": {
                        "col_1": {
                            "name": "col_1",
                            "constraints": [{"type": "primary_key"}],
                        },
                    },
                },
                check_passes,
                id="single_primary_key",
            ),
            pytest.param(
                {
                    "columns": {
                        "col_1": {
                            "name": "col_1",
                            "constraints": [{"type": "not_null"}],
                        },
                        "col_2": {
                            "name": "col_2",
                            "constraints": [{"type": "not_null"}],
                        },
                    },
                },
                check_passes,
                id="no_primary_key_constraints",
            ),
            pytest.param(
                {
                    "columns": {
                        "col_1": {"name": "col_1", "constraints": [{"type": None}]}
                    }
                },
                check_passes,
                id="constraint_type_is_none",
            ),
            # A single column carrying a duplicate primary_key constraint is
            # counted once, because the inner loop breaks on the first match.
            pytest.param(
                {
                    "columns": {
                        "col_1": {
                            "name": "col_1",
                            "constraints": [
                                {"type": "primary_key"},
                                {"type": "primary_key"},
                            ],
                        },
                    },
                },
                check_passes,
                id="one_column_with_duplicate_primary_key",
            ),
            pytest.param(
                {
                    "columns": {
                        "col_1": {
                            "name": "col_1",
                            "constraints": [{"type": "primary_key"}],
                        },
                        "col_2": {
                            "name": "col_2",
                            "constraints": [{"type": "primary_key"}],
                        },
                    },
                },
                check_fails,
                id="two_primary_key_columns",
            ),
            pytest.param(
                {
                    "columns": {
                        "col_1": {
                            "name": "col_1",
                            "constraints": [
                                {"type": "primary_key"},
                                {"type": "not_null"},
                            ],
                        },
                        "col_2": {
                            "name": "col_2",
                            "constraints": [{"type": "primary_key"}],
                        },
                        "col_3": {
                            "name": "col_3",
                            "constraints": [{"type": "primary_key"}],
                        },
                    },
                },
                check_fails,
                id="three_primary_key_columns",
            ),
        ],
    )
    def test_check_model_single_primary_key(self, model_override, check_fn):
        check_fn("check_model_single_primary_key", model=model_override)

    def test_enum_constraint_type_is_read_via_its_value(self):
        check_fails(
            "check_model_single_primary_key",
            model={
                "columns": {
                    "col_1": SimpleNamespace(
                        constraints=[SimpleNamespace(type=_ConstraintType.PRIMARY_KEY)]
                    ),
                    "col_2": SimpleNamespace(
                        constraints=[SimpleNamespace(type=_ConstraintType.PRIMARY_KEY)]
                    ),
                },
            },
            match=r"has more than one column-level primary key constraint: \['col_1', 'col_2'\]",
        )
