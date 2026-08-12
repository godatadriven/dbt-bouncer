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


def _typed_cols(**names_to_types: str | None) -> dict:
    """Build a `columns` dict from column name -> declared `data_type`.

    A `None` type produces a column with no `data_type` declared at all,
    mirroring a properties file that omits the key.

    Returns:
        dict: A `columns` mapping keyed by column name.

    """
    return {
        name: {"name": name}
        if data_type is None
        else {"name": name, "data_type": data_type}
        for name, data_type in names_to_types.items()
    }


def _col_test(column_name: str, *, name: str = "not_null") -> dict:
    """Build a column-level test node attached to the default model.

    Returns:
        dict: A manifest test node dict.

    """
    return {"column_name": column_name, "test_metadata": {"name": name}}


class TestCheckModelColumnHasSpecifiedTest:
    @pytest.mark.parametrize(
        ("model_override", "ctx_tests", "check_fn"),
        [
            pytest.param(
                {"columns": _cols("is_active")},
                [_col_test("is_active")],
                check_passes,
                id="matching_column_has_test",
            ),
            pytest.param(
                {"columns": _cols("is_active", "customer_id")},
                [_col_test("is_active")],
                check_passes,
                id="non_matching_column_needs_no_test",
            ),
            pytest.param(
                {"columns": _cols("customer_id")},
                [],
                check_passes,
                id="no_columns_match_pattern",
            ),
            pytest.param({"columns": {}}, [], check_passes, id="no_columns"),
            pytest.param({"columns": None}, [], check_passes, id="columns_is_none"),
            pytest.param(
                {"columns": _cols("is_active")},
                [],
                check_fails,
                id="matching_column_has_no_tests",
            ),
            pytest.param(
                {"columns": _cols("is_active")},
                [_col_test("is_active", name="unique")],
                check_fails,
                id="matching_column_has_a_different_test",
            ),
            pytest.param(
                {"columns": _cols("is_active", "is_deleted")},
                [_col_test("is_active")],
                check_fails,
                id="one_of_two_matching_columns_untested",
            ),
            # A model-level test carries no `column_name`, so it must not be
            # treated as covering a column.
            pytest.param(
                {"columns": _cols("is_active")},
                [{"column_name": "", "test_metadata": {"name": "not_null"}}],
                check_fails,
                id="model_level_test_does_not_count",
            ),
            pytest.param(
                {"columns": _cols("is_active")},
                [{"column_name": "is_active", "test_metadata": None}],
                check_fails,
                id="test_without_test_metadata",
            ),
        ],
    )
    def test_check_model_column_has_specified_test(
        self, model_override, ctx_tests, check_fn
    ):
        check_fn(
            "check_model_column_has_specified_test",
            column_name_pattern="^is_.*",
            test_name="not_null",
            model=model_override,
            ctx_tests=ctx_tests,
        )

    def test_failure_message_lists_untested_columns(self):
        check_fails(
            "check_model_column_has_specified_test",
            column_name_pattern="^is_.*",
            test_name="not_null",
            model={"columns": _cols("is_active", "is_deleted")},
            ctx_tests=[],
            match=r"has columns that should have a `not_null` test: \['is_active', 'is_deleted'\]",
        )

    def test_tests_attached_to_another_model_are_ignored(self):
        # `tests_by_attached_node` is keyed by `attached_node`, so a test on a
        # different model must not satisfy this model's requirement.
        check_fails(
            "check_model_column_has_specified_test",
            column_name_pattern="^is_.*",
            test_name="not_null",
            model={"columns": _cols("is_active")},
            ctx_tests=[
                {
                    "attached_node": "model.package_name.model_2",
                    "column_name": "is_active",
                    "test_metadata": {"name": "not_null"},
                },
            ],
        )


class TestCheckModelColumnDescriptionPopulated:
    @pytest.mark.parametrize(
        ("min_description_length", "model_override", "check_fn"),
        [
            pytest.param(
                None,
                {"columns": {"col_1": {"name": "col_1", "description": "A column."}}},
                check_passes,
                id="populated_description",
            ),
            pytest.param(None, {"columns": {}}, check_passes, id="no_columns"),
            pytest.param(None, {"columns": None}, check_passes, id="columns_is_none"),
            pytest.param(
                25,
                {
                    "columns": {
                        "col_1": {
                            "name": "col_1",
                            "description": "A description that is comfortably long enough.",
                        },
                    },
                },
                check_passes,
                id="meets_custom_min_description_length",
            ),
            pytest.param(
                None,
                {"columns": {"col_1": {"name": "col_1", "description": ""}}},
                check_fails,
                id="empty_description",
            ),
            pytest.param(
                None,
                {"columns": {"col_1": {"name": "col_1"}}},
                check_fails,
                id="description_absent",
            ),
            pytest.param(
                None,
                {"columns": {"col_1": {"name": "col_1", "description": "n/a"}}},
                check_fails,
                id="placeholder_description",
            ),
            pytest.param(
                None,
                {"columns": {"col_1": {"name": "col_1", "description": "abc"}}},
                check_fails,
                id="shorter_than_default_min_length",
            ),
            pytest.param(
                25,
                {"columns": {"col_1": {"name": "col_1", "description": "A column."}}},
                check_fails,
                id="shorter_than_custom_min_description_length",
            ),
        ],
    )
    def test_check_model_column_description_populated(
        self, min_description_length, model_override, check_fn
    ):
        check_fn(
            "check_model_column_description_populated",
            min_description_length=min_description_length,
            model=model_override,
        )

    def test_failure_message_lists_undocumented_columns(self):
        check_fails(
            "check_model_column_description_populated",
            model={
                "columns": {
                    "col_1": {"name": "col_1", "description": ""},
                    "col_2": {"name": "col_2", "description": "A documented column."},
                    "col_3": {"name": "col_3"},
                },
            },
            match=r"has columns that do not have a populated description: \['col_1', 'col_3'\]",
        )


class TestCheckModelColumnNameCompliesToColumnType:
    @pytest.mark.parametrize(
        ("kwargs", "model_override", "check_fn"),
        [
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": _typed_cols(is_active="BOOLEAN")},
                check_passes,
                id="types_complying",
            ),
            pytest.param(
                {"type_pattern": "^BOOL"},
                {"columns": _typed_cols(is_active="BOOLEAN")},
                check_passes,
                id="type_pattern_complying",
            ),
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": _typed_cols(customer_id="INTEGER")},
                check_passes,
                id="column_name_does_not_match_pattern",
            ),
            # `data_type` is optional in a properties file, so a column without
            # one is skipped rather than failed (see check_model_columns_have_types).
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": _typed_cols(is_active=None)},
                check_passes,
                id="no_declared_data_type_is_skipped",
            ),
            pytest.param(
                {"type_pattern": "^BOOL"},
                {"columns": _typed_cols(is_active=None)},
                check_passes,
                id="no_declared_data_type_is_skipped_type_pattern",
            ),
            pytest.param(
                {"types": ["BOOLEAN"]}, {"columns": {}}, check_passes, id="no_columns"
            ),
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": None},
                check_passes,
                id="columns_is_none",
            ),
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": _typed_cols(is_active="INTEGER")},
                check_fails,
                id="types_non_complying",
            ),
            pytest.param(
                {"type_pattern": "^BOOL"},
                {"columns": _typed_cols(is_active="INTEGER")},
                check_fails,
                id="type_pattern_non_complying",
            ),
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": _typed_cols(is_active="BOOLEAN", is_deleted="INTEGER")},
                check_fails,
                id="one_of_two_matching_columns_non_complying",
            ),
        ],
    )
    def test_check_model_column_name_complies_to_column_type(
        self, kwargs, model_override, check_fn
    ):
        check_fn(
            "check_model_column_name_complies_to_column_type",
            column_name_pattern="^is_.*",
            model=model_override,
            **kwargs,
        )

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            pytest.param(
                {},
                r"Either 'type_pattern' or 'types' must be supplied\.",
                id="neither_supplied",
            ),
            pytest.param(
                {"type_pattern": "^BOOL", "types": ["BOOLEAN"]},
                r"Only one of 'type_pattern' or 'types' can be supplied\.",
                id="both_supplied",
            ),
        ],
    )
    def test_type_arguments_are_mutually_exclusive_and_required(self, kwargs, match):
        check_fails(
            "check_model_column_name_complies_to_column_type",
            expected_exception=ValueError,
            match=match,
            column_name_pattern="^is_.*",
            model={"columns": _typed_cols(is_active="BOOLEAN")},
            **kwargs,
        )

    def test_failure_message_lists_non_complying_columns(self):
        check_fails(
            "check_model_column_name_complies_to_column_type",
            column_name_pattern="^is_.*",
            types=["BOOLEAN"],
            model={"columns": _typed_cols(is_active="INTEGER")},
            match=r"has columns matching `\^is_\.\*` whose declared data type is not in \['BOOLEAN'\]: \['is_active'\]",
        )


class TestCheckModelColumnTypeCompliesToColumnName:
    @pytest.mark.parametrize(
        ("kwargs", "model_override", "check_fn"),
        [
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": _typed_cols(is_active="BOOLEAN")},
                check_passes,
                id="types_complying",
            ),
            pytest.param(
                {"type_pattern": "^BOOL"},
                {"columns": _typed_cols(is_active="BOOLEAN")},
                check_passes,
                id="type_pattern_complying",
            ),
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": _typed_cols(customer_id="INTEGER")},
                check_passes,
                id="type_not_in_scope",
            ),
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": _typed_cols(customer_id=None)},
                check_passes,
                id="no_declared_data_type_is_skipped",
            ),
            pytest.param(
                {"type_pattern": ".*"},
                {"columns": _typed_cols(customer_id=None)},
                check_passes,
                id="no_declared_data_type_is_skipped_type_pattern",
            ),
            pytest.param(
                {"types": ["BOOLEAN"]}, {"columns": {}}, check_passes, id="no_columns"
            ),
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": None},
                check_passes,
                id="columns_is_none",
            ),
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": _typed_cols(active_flag="BOOLEAN")},
                check_fails,
                id="types_non_complying_name",
            ),
            pytest.param(
                {"type_pattern": "^BOOL"},
                {"columns": _typed_cols(active_flag="BOOLEAN")},
                check_fails,
                id="type_pattern_non_complying_name",
            ),
            pytest.param(
                {"types": ["BOOLEAN"]},
                {"columns": _typed_cols(is_active="BOOLEAN", active_flag="BOOLEAN")},
                check_fails,
                id="one_of_two_typed_columns_non_complying",
            ),
        ],
    )
    def test_check_model_column_type_complies_to_column_name(
        self, kwargs, model_override, check_fn
    ):
        check_fn(
            "check_model_column_type_complies_to_column_name",
            column_name_pattern="^is_.*",
            model=model_override,
            **kwargs,
        )

    @pytest.mark.parametrize(
        ("kwargs", "match"),
        [
            pytest.param(
                {},
                r"Either 'type_pattern' or 'types' must be supplied\.",
                id="neither_supplied",
            ),
            pytest.param(
                {"type_pattern": "^BOOL", "types": ["BOOLEAN"]},
                r"Only one of 'type_pattern' or 'types' can be supplied\.",
                id="both_supplied",
            ),
        ],
    )
    def test_type_arguments_are_mutually_exclusive_and_required(self, kwargs, match):
        check_fails(
            "check_model_column_type_complies_to_column_name",
            expected_exception=ValueError,
            match=match,
            column_name_pattern="^is_.*",
            model={"columns": _typed_cols(is_active="BOOLEAN")},
            **kwargs,
        )

    def test_failure_message_lists_non_complying_columns(self):
        check_fails(
            "check_model_column_type_complies_to_column_name",
            column_name_pattern="^is_.*",
            types=["BOOLEAN"],
            model={"columns": _typed_cols(active_flag="BOOLEAN")},
            match=r"has columns with declared types in \['BOOLEAN'\] that don't comply with the specified naming pattern \(`\^is_\.\*`\): \['active_flag'\]",
        )


class TestCheckModelColumnNames:
    @pytest.mark.parametrize(
        ("column_name_pattern", "model_override", "check_fn"),
        [
            pytest.param(
                "[a-z_]*",
                {"columns": _cols("customer_id", "is_active")},
                check_passes,
                id="all_columns_match",
            ),
            pytest.param("[a-z_]*", {"columns": {}}, check_passes, id="no_columns"),
            pytest.param(
                "[a-z_]*", {"columns": None}, check_passes, id="columns_is_none"
            ),
            pytest.param(
                "[a-z_]*",
                {"columns": _cols("CustomerId")},
                check_fails,
                id="column_does_not_match",
            ),
            # The pattern is applied with `fullmatch`, so a partial match fails.
            pytest.param(
                "^customer",
                {"columns": _cols("customer_id")},
                check_fails,
                id="partial_match_is_not_enough",
            ),
            pytest.param(
                "[a-z_]*",
                {"columns": _cols("customer_id", "CustomerId")},
                check_fails,
                id="one_of_two_columns_does_not_match",
            ),
        ],
    )
    def test_check_model_column_names(
        self, column_name_pattern, model_override, check_fn
    ):
        check_fn(
            "check_model_column_names",
            column_name_pattern=column_name_pattern,
            model=model_override,
        )

    def test_failure_message_lists_non_complying_columns(self):
        check_fails(
            "check_model_column_names",
            column_name_pattern="[a-z_]*",
            model={"columns": _cols("CustomerId", "customer_id")},
            match=r"has columns \(\['CustomerId'\]\) that do not match the supplied regex: `\[a-z_\]\*`\.",
        )
