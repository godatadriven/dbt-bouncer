from types import SimpleNamespace
from typing import Any

import pytest
from pydantic import Field

from dbt_bouncer.check_framework.exceptions import DbtBouncerFailedCheckError
from dbt_bouncer.check_framework.patterns import (
    BaseColumnsHaveTypesCheck,
    BaseDescriptionPopulatedCheck,
    BaseHasMetaKeysCheck,
    BaseHasTagsCheck,
    BaseHasUnitTestsCheck,
    BaseNamePatternCheck,
)
from dbt_bouncer.enums import Criteria

# These ABCs are the shared implementation behind the class-based check API that
# `docs/CONTRIBUTING.md` documents for plugin authors. The concrete subclasses
# below are the smallest thing that satisfies each ABC's abstract properties, so
# the tests exercise the template `execute()` rather than any built-in check.


class _NamePatternCheck(BaseNamePatternCheck):
    model: Any = Field(default=None)
    model_name_pattern: str

    @property
    def _name_pattern(self) -> str:
        return self.model_name_pattern

    @property
    def _resource_name(self) -> str:
        return self.model.name

    @property
    def _resource_display_name(self) -> str:
        return self.model.name


class _DescriptionPopulatedCheck(BaseDescriptionPopulatedCheck):
    model: Any = Field(default=None)

    @property
    def _resource_description(self) -> str:
        return self.model.description

    @property
    def _resource_display_name(self) -> str:
        return self.model.name


class _ColumnsHaveTypesCheck(BaseColumnsHaveTypesCheck):
    model: Any = Field(default=None)

    @property
    def _resource_columns(self) -> dict[str, Any]:
        return self.model.columns

    @property
    def _resource_display_name(self) -> str:
        return self.model.name


class _HasUnitTestsCheck(BaseHasUnitTestsCheck):
    model: Any = Field(default=None)

    @property
    def _resource_unique_id(self) -> str:
        return self.model.unique_id

    @property
    def _resource_display_name(self) -> str:
        return self.model.name


class _HasTagsCheck(BaseHasTagsCheck):
    model: Any = Field(default=None)

    @property
    def _resource_tags(self) -> list[str]:
        return self.model.tags

    @property
    def _resource_display_name(self) -> str:
        return self.model.name


class _HasMetaKeysCheck(BaseHasMetaKeysCheck):
    model: Any = Field(default=None)

    @property
    def _resource_meta(self) -> dict[str, Any]:
        return self.model.meta

    @property
    def _resource_display_name(self) -> str:
        return self.model.name


def _model(**kwargs) -> SimpleNamespace:
    kwargs.setdefault("name", "a_model")
    return SimpleNamespace(**kwargs)


def _column(data_type: str | None) -> SimpleNamespace:
    return SimpleNamespace(data_type=data_type)


def _unit_test(depends_on_nodes: list[str] | None) -> SimpleNamespace:
    return SimpleNamespace(
        depends_on=SimpleNamespace(nodes=depends_on_nodes)
        if depends_on_nodes is not None
        else None,
        unique_id="unit_test.pkg.a_model.ut",
    )


class TestBaseNamePatternCheck:
    """Tests for BaseNamePatternCheck."""

    @pytest.mark.parametrize(
        ("pattern", "name"),
        [
            pytest.param("^stg_", "stg_orders", id="prefix_matches"),
            pytest.param("^stg_[a-z]+$", "stg_orders", id="full_pattern_matches"),
            # `match()` anchors at the start only, so a trailing mismatch passes.
            pytest.param("^stg_", "stg_orders_v2", id="unanchored_suffix_matches"),
            pytest.param("  ^stg_  ", "stg_orders", id="pattern_whitespace_stripped"),
        ],
    )
    def test_passes(self, pattern, name):
        """A resource name matching the pattern does not raise."""
        _NamePatternCheck(model_name_pattern=pattern, model=_model(name=name)).execute()

    @pytest.mark.parametrize(
        ("pattern", "name"),
        [
            pytest.param("^stg_", "orders", id="no_match"),
            # `match()` anchors at the start, so a mid-string hit is not enough.
            pytest.param("^stg_", "int_stg_orders", id="match_not_at_start"),
            pytest.param("^stg_[a-z]+$", "stg_orders_1", id="trailing_anchor_fails"),
        ],
    )
    def test_fails(self, pattern, name):
        """A resource name not matching the pattern raises."""
        check = _NamePatternCheck(model_name_pattern=pattern, model=_model(name=name))

        with pytest.raises(DbtBouncerFailedCheckError, match="does not match"):
            check.execute()

    def test_failure_message_reports_the_stripped_pattern(self):
        """The error message quotes the pattern without its surrounding whitespace."""
        check = _NamePatternCheck(
            model_name_pattern="  ^stg_  ", model=_model(name="orders")
        )

        with pytest.raises(
            DbtBouncerFailedCheckError,
            match=r"`orders` does not match the supplied regex `\^stg_`\.",
        ):
            check.execute()

    def test_non_string_resource_name_is_coerced(self):
        """A non-string resource name is coerced before matching rather than raising a TypeError."""
        _NamePatternCheck(model_name_pattern="^123$", model=_model(name=123)).execute()


class TestBaseDescriptionPopulatedCheck:
    """Tests for BaseDescriptionPopulatedCheck."""

    @pytest.mark.parametrize(
        ("description", "min_description_length"),
        [
            pytest.param("A real description.", None, id="default_minimum"),
            pytest.param("abcd", None, id="exactly_default_minimum"),
            pytest.param("abc", 2, id="explicit_lower_minimum"),
            pytest.param("abcdefghij", 10, id="exactly_explicit_minimum"),
        ],
    )
    def test_passes(self, description, min_description_length):
        """A populated description does not raise."""
        _DescriptionPopulatedCheck(
            min_description_length=min_description_length,
            model=_model(description=description),
        ).execute()

    @pytest.mark.parametrize(
        ("description", "min_description_length"),
        [
            pytest.param("", None, id="empty"),
            pytest.param("   ", None, id="whitespace_only"),
            pytest.param("abc", None, id="shorter_than_default_minimum"),
            pytest.param("N/A", None, id="placeholder"),
            pytest.param("abcdefghi", 10, id="shorter_than_explicit_minimum"),
        ],
    )
    def test_fails(self, description, min_description_length):
        """An unpopulated description raises."""
        check = _DescriptionPopulatedCheck(
            min_description_length=min_description_length,
            model=_model(description=description),
        )

        with pytest.raises(
            DbtBouncerFailedCheckError,
            match=r"`a_model` does not have a populated description\.",
        ):
            check.execute()


class TestBaseColumnsHaveTypesCheck:
    """Tests for BaseColumnsHaveTypesCheck."""

    @pytest.mark.parametrize(
        "columns",
        [
            pytest.param({}, id="no_columns"),
            pytest.param({"col_1": _column("VARCHAR")}, id="single_typed_column"),
            pytest.param(
                {"col_1": _column("VARCHAR"), "col_2": _column("INTEGER")},
                id="all_columns_typed",
            ),
        ],
    )
    def test_passes(self, columns):
        """A resource whose every column declares a data_type does not raise."""
        _ColumnsHaveTypesCheck(model=_model(columns=columns)).execute()

    @pytest.mark.parametrize(
        ("columns", "match"),
        [
            pytest.param({"col_1": _column(None)}, r"\['col_1'\]", id="data_type_none"),
            pytest.param({"col_1": _column("")}, r"\['col_1'\]", id="data_type_empty"),
            pytest.param(
                {"col_1": _column("VARCHAR"), "col_2": _column(None)},
                r"\['col_2'\]",
                id="only_untyped_column_reported",
            ),
            pytest.param(
                {"col_1": _column(None), "col_2": _column(None)},
                r"\['col_1', 'col_2'\]",
                id="every_untyped_column_reported",
            ),
        ],
    )
    def test_fails(self, columns, match):
        """A resource with an untyped column raises and names the offending columns."""
        check = _ColumnsHaveTypesCheck(model=_model(columns=columns))

        with pytest.raises(DbtBouncerFailedCheckError, match=match):
            check.execute()


class TestBaseHasUnitTestsCheck:
    """Tests for BaseHasUnitTestsCheck."""

    @staticmethod
    def _build(unit_tests, **kwargs):
        check = _HasUnitTestsCheck(
            model=_model(unique_id="model.pkg.a_model"),
            **kwargs,
        )
        check.set_context(SimpleNamespace(unit_tests=unit_tests))
        return check

    @pytest.mark.parametrize(
        ("unit_tests", "kwargs"),
        [
            pytest.param(
                [_unit_test(["model.pkg.a_model"])],
                {},
                id="exactly_default_minimum",
            ),
            pytest.param(
                [_unit_test(["model.pkg.a_model"]), _unit_test(["model.pkg.a_model"])],
                {"min_number_of_unit_tests": 2},
                id="exactly_explicit_minimum",
            ),
            pytest.param(
                [],
                {"min_number_of_unit_tests": 0},
                id="minimum_of_zero_needs_no_unit_tests",
            ),
        ],
    )
    def test_passes(self, unit_tests, kwargs):
        """A resource with enough unit tests does not raise."""
        self._build(unit_tests, **kwargs).execute()

    @pytest.mark.parametrize(
        ("unit_tests", "kwargs", "match"),
        [
            pytest.param([], {}, "has 0 unit tests", id="no_unit_tests"),
            pytest.param(
                [_unit_test(["model.pkg.other_model"])],
                {},
                "has 0 unit tests",
                id="unit_test_for_a_different_model",
            ),
            pytest.param(
                [_unit_test(None)],
                {},
                "has 0 unit tests",
                id="unit_test_without_depends_on",
            ),
            pytest.param(
                [_unit_test([])],
                {},
                "has 0 unit tests",
                id="unit_test_with_empty_depends_on",
            ),
            pytest.param(
                [_unit_test(["model.pkg.a_model"])],
                {"min_number_of_unit_tests": 2},
                "has 1 unit tests, this is less than the minimum of 2",
                id="fewer_than_explicit_minimum",
            ),
            # Only the first entry in `depends_on.nodes` is considered, so a unit
            # test that reaches this model only indirectly does not count.
            pytest.param(
                [_unit_test(["model.pkg.other_model", "model.pkg.a_model"])],
                {},
                "has 0 unit tests",
                id="model_not_first_in_depends_on",
            ),
        ],
    )
    def test_fails(self, unit_tests, kwargs, match):
        """A resource without enough unit tests raises."""
        check = self._build(unit_tests, **kwargs)

        with pytest.raises(DbtBouncerFailedCheckError, match=match):
            check.execute()


class TestBaseHasTagsCheck:
    """Tests for BaseHasTagsCheck."""

    @pytest.mark.parametrize(
        ("criteria", "required_tags", "resource_tags"),
        [
            pytest.param(None, ["a", "b"], ["a", "b"], id="default_criteria_is_all"),
            pytest.param(Criteria.ALL, ["a"], ["a", "b"], id="all_subset"),
            pytest.param(Criteria.ALL, [], [], id="all_no_required_tags"),
            pytest.param(Criteria.ANY, ["a", "b"], ["b"], id="any_one_present"),
            pytest.param(Criteria.ANY, ["a"], ["a", "b"], id="any_with_extras"),
            pytest.param(Criteria.ONE, ["a", "b"], ["a"], id="one_exactly_one"),
            pytest.param(Criteria.ONE, ["a", "b"], ["b", "c"], id="one_with_extras"),
        ],
    )
    def test_passes(self, criteria, required_tags, resource_tags):
        """Tags satisfying the criteria do not raise."""
        kwargs = {} if criteria is None else {"criteria": criteria}
        _HasTagsCheck(
            tags=required_tags, model=_model(tags=resource_tags), **kwargs
        ).execute()

    @pytest.mark.parametrize(
        ("criteria", "required_tags", "resource_tags", "match"),
        [
            pytest.param(
                Criteria.ALL,
                ["a", "b"],
                ["a"],
                r"is missing required tags: \['b'\]",
                id="all_one_missing",
            ),
            pytest.param(
                Criteria.ALL,
                ["a", "b"],
                [],
                r"is missing required tags: \['a', 'b'\]",
                id="all_none_present",
            ),
            pytest.param(
                Criteria.ANY,
                ["a", "b"],
                ["c"],
                r"does not have any of the required tags: \['a', 'b'\]",
                id="any_none_present",
            ),
            pytest.param(
                Criteria.ANY,
                ["a"],
                [],
                r"does not have any of the required tags: \['a'\]",
                id="any_no_tags_at_all",
            ),
            pytest.param(
                Criteria.ONE,
                ["a", "b"],
                ["a", "b"],
                r"must have exactly one of the required tags: \['a', 'b'\]",
                id="one_too_many",
            ),
            pytest.param(
                Criteria.ONE,
                ["a", "b"],
                ["c"],
                r"must have exactly one of the required tags: \['a', 'b'\]",
                id="one_none_present",
            ),
        ],
    )
    def test_fails(self, criteria, required_tags, resource_tags, match):
        """Tags failing the criteria raise a message naming the criteria."""
        check = _HasTagsCheck(
            criteria=criteria, tags=required_tags, model=_model(tags=resource_tags)
        )

        with pytest.raises(DbtBouncerFailedCheckError, match=match):
            check.execute()


class TestBaseHasMetaKeysCheck:
    """Tests for BaseHasMetaKeysCheck."""

    @pytest.mark.parametrize(
        ("keys", "meta"),
        [
            pytest.param(["owner"], {"owner": "Bob"}, id="top_level_key"),
            pytest.param(["owner"], {"owner": "Bob", "extra": 1}, id="with_extras"),
            pytest.param([], {}, id="no_required_keys"),
            pytest.param(
                [{"owner": ["name"]}],
                {"owner": {"name": "Bob"}},
                id="nested_key",
            ),
        ],
    )
    def test_passes(self, keys, meta):
        """Meta carrying every required key does not raise."""
        _HasMetaKeysCheck(keys=keys, model=_model(meta=meta)).execute()

    @pytest.mark.parametrize(
        ("keys", "meta", "match"),
        [
            pytest.param(["owner"], {}, r"\['owner'\]", id="empty_meta"),
            pytest.param(
                ["owner"], {"maturity": "high"}, r"\['owner'\]", id="different_key"
            ),
            pytest.param(
                ["owner", "maturity"],
                {},
                r"\['owner', 'maturity'\]",
                id="several_missing",
            ),
            # `>>` markers used internally to flag a missing level are stripped
            # from the message, leaving the `>`-joined path.
            pytest.param(
                [{"owner": ["name"]}],
                {"owner": {"email": "bob@example.com"}},
                r"\['owner>name'\]",
                id="nested_key_path_in_message",
            ),
        ],
    )
    def test_fails(self, keys, meta, match):
        """Meta missing a required key raises and names the missing paths."""
        check = _HasMetaKeysCheck(keys=keys, model=_model(meta=meta))

        with pytest.raises(DbtBouncerFailedCheckError, match=match):
            check.execute()
