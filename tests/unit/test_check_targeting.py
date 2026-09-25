"""Tests for which resources a check targets, resolved against a real manifest.

`test_selectors.py` and the `resource_in_path` / `_check_applies_to_resource`
tests in `test_runner.py` cover selectors, path patterns and materialization one
at a time, against hand-built objects. These tests load the committed dbt_112
fixture, build the context the way `run_bouncer` does, and assert the exact set
of resources `_assemble_checks_to_run` hands to the executor. That catches
mistakes in how the filters combine, and in graph walks over a real DAG.

The DAG they rely on (models only, all in `dbt_bouncer_test_project`):

    seed raw_orders   -> stg_orders (view, crm) -> orders (table)
                                                -> customers v1-v3 (table)
                                                -> py_order_stats (table)
                      -> stg_raw_orders_for_snapshot (ephemeral, crm)
    seed raw_payments -> stg_payments (view)    -> orders, customers v1-v3
    seed raw_customers,
    source dummy_source.customers
                      -> stg_customers (view, crm) -> customers v1-v3
"""

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from dbt_bouncer.cli.run.utils import (
    _context_from_config,
    _parse_only,
    _prepare_bouncer_config,
    run_bouncer,
)
from dbt_bouncer.enums import ConfigFileSource, ExitCode
from dbt_bouncer.exceptions import DbtBouncerConfigError
from dbt_bouncer.main import app
from dbt_bouncer.runner import _assemble_checks_to_run

_DBT_112_TARGET = Path("tests/fixtures/dbt_112/target").resolve()
_MODEL_PREFIX = "model.dbt_bouncer_test_project."

# Fails on every model it targets, so the targeted set is also the failing set.
_CHECK = {"name": "check_model_names", "model_name_pattern": "^zzz_"}

_ALL_MODELS = {
    "customers.v1",
    "customers.v2",
    "customers.v3",
    "int_disabled_model",
    "int_exclude_via_meta_config",
    "int_incremental",
    "int_model_1",
    "metricflow_time_spine",
    "orders",
    "py_order_stats",
    "stg_customers",
    "stg_orders",
    "stg_payments",
    "stg_raw_orders_for_snapshot",
}
_CUSTOMERS = {"customers.v1", "customers.v2", "customers.v3"}
_STAGING = {
    "stg_customers",
    "stg_orders",
    "stg_payments",
    "stg_raw_orders_for_snapshot",
}


def _write_config(tmp_path: Path, checks: list[dict], **global_settings) -> Path:
    config_file = tmp_path / "dbt-bouncer.yml"
    config_file.write_text(
        yaml.dump(
            {
                "dbt_artifacts_dir": str(_DBT_112_TARGET),
                "manifest_checks": checks,
                **global_settings,
            }
        ),
        encoding="utf-8",
    )
    return config_file


def _targeted(tmp_path: Path, check_settings: dict, **global_settings) -> set[str]:
    """Return the resources one `check_model_names` check would run against.

    Returns:
        set[str]: Model unique IDs with the project prefix stripped (other
            resource types keep their full unique ID).

    """
    config_file = _write_config(
        tmp_path, [{**_CHECK, **check_settings}], **global_settings
    )
    bouncer_config, categories, config_path = _prepare_bouncer_config(
        config_file, ConfigFileSource.COMMANDLINE, _parse_only(""), set(), None
    )
    ctx = _context_from_config(bouncer_config, categories, config_path)
    return {
        str(e["unique_id"]).removeprefix(_MODEL_PREFIX)
        for e in _assemble_checks_to_run(ctx)
    }


def test_no_filter_targets_every_project_model(tmp_path):
    """Without a filter, every model in the project is checked (other packages are not)."""
    assert _targeted(tmp_path, {}) == _ALL_MODELS


class TestSelectorAgainstRealDag:
    """Each selector method and graph operator, resolved over the dbt_112 DAG."""

    @pytest.mark.parametrize(
        ("selector", "expected"),
        [
            pytest.param(
                "tag:crm",
                {"stg_customers", "stg_orders", "stg_raw_orders_for_snapshot"},
                id="tag",
            ),
            pytest.param(
                "path:models/marts", {*_CUSTOMERS, "orders"}, id="path_directory"
            ),
            pytest.param(
                "path:models/staging/*/stg_*s.sql",
                {"stg_customers", "stg_orders", "stg_payments"},
                id="path_glob",
            ),
            pytest.param("fqn:*.staging.*", _STAGING, id="fqn_glob"),
            pytest.param(
                "config.materialized:ephemeral",
                {"int_disabled_model", "int_model_1", "stg_raw_orders_for_snapshot"},
                id="config",
            ),
            pytest.param("package:dbt_bouncer_test_project", _ALL_MODELS, id="package"),
            pytest.param("stg_orders", {"stg_orders"}, id="name"),
            pytest.param(
                "int_*",
                {m for m in _ALL_MODELS if m.startswith("int_")},
                id="name_glob",
            ),
            # Versioned models share a name, so a name atom selects every version.
            pytest.param("customers", _CUSTOMERS, id="name_of_versioned_model"),
        ],
    )
    def test_method(self, expected, selector, tmp_path):
        """A method atom selects exactly the resources whose property matches."""
        assert _targeted(tmp_path, {"selector": selector}) == expected

    @pytest.mark.parametrize(
        ("selector", "expected"),
        [
            # Seeds are ancestors too, but this check only runs on models.
            pytest.param(
                "+orders", {"orders", "stg_orders", "stg_payments"}, id="ancestors"
            ),
            pytest.param(
                "stg_orders+",
                {"stg_orders", "orders", "py_order_stats", *_CUSTOMERS},
                id="descendants",
            ),
            pytest.param(
                "+stg_payments+",
                {"stg_payments", "orders", *_CUSTOMERS},
                id="both",
            ),
            # The seed itself is not a model, so only its descendants are checked.
            pytest.param(
                "raw_orders+",
                {
                    "stg_orders",
                    "stg_raw_orders_for_snapshot",
                    "orders",
                    "py_order_stats",
                    *_CUSTOMERS,
                },
                id="descendants_of_a_seed",
            ),
            pytest.param(
                "raw_orders+1",
                {"stg_orders", "stg_raw_orders_for_snapshot"},
                id="descendant_degree_stops_after_one_hop",
            ),
            # `@` also pulls in the other parents of `stg_payments`'s children.
            pytest.param(
                "@stg_payments",
                {"stg_payments", "stg_orders", "stg_customers", "orders", *_CUSTOMERS},
                id="at",
            ),
            pytest.param(
                "+tag:crm",
                {"stg_customers", "stg_orders", "stg_raw_orders_for_snapshot"},
                id="graph_operator_wrapping_a_method",
            ),
            pytest.param(
                "tag:crm+",
                {
                    "stg_customers",
                    "stg_orders",
                    "stg_raw_orders_for_snapshot",
                    "orders",
                    "py_order_stats",
                    *_CUSTOMERS,
                },
                id="descendants_of_a_tag",
            ),
        ],
    )
    def test_graph_operator(self, expected, selector, tmp_path):
        """Graph operators walk `parent_map` / `child_map` from the matched seeds."""
        assert _targeted(tmp_path, {"selector": selector}) == expected

    @pytest.mark.parametrize(
        ("selector", "expected"),
        [
            pytest.param(
                "tag:crm,config.materialized:view",
                {"stg_customers", "stg_orders"},
                id="intersection",
            ),
            pytest.param(
                "tag:crm stg_payments",
                {
                    "stg_customers",
                    "stg_orders",
                    "stg_payments",
                    "stg_raw_orders_for_snapshot",
                },
                id="union",
            ),
            # `stg_orders+` intersected with `path:models/marts`: only the marts.
            pytest.param(
                "stg_orders+,path:models/marts",
                {"orders", *_CUSTOMERS},
                id="intersection_with_a_graph_operator",
            ),
            pytest.param(
                "+orders,tag:crm stg_payments+,customers",
                {"stg_orders", *_CUSTOMERS},
                id="union_of_intersections",
            ),
            pytest.param(
                "tag:crm,path:models/marts", set(), id="disjoint_intersection"
            ),
            pytest.param("tag:does_not_exist", set(), id="no_match"),
        ],
    )
    def test_set_operation(self, expected, selector, tmp_path):
        """Spaces union atoms and commas intersect them, as in dbt."""
        assert _targeted(tmp_path, {"selector": selector}) == expected

    def test_selector_applies_to_non_model_resources(self, tmp_path):
        """A source check with `+stg_customers` targets only the source it reads from."""
        config_file = _write_config(
            tmp_path,
            [
                {
                    "name": "check_source_description_populated",
                    "selector": "+stg_customers",
                }
            ],
        )
        bouncer_config, categories, config_path = _prepare_bouncer_config(
            config_file, ConfigFileSource.COMMANDLINE, _parse_only(""), set(), None
        )
        ctx = _context_from_config(bouncer_config, categories, config_path)

        assert {e["unique_id"] for e in _assemble_checks_to_run(ctx)} == {
            "source.dbt_bouncer_test_project.dummy_source.customers"
        }

    def test_global_selector_applies_to_checks_without_one(self, tmp_path):
        """A top-level `selector` is copied onto a check that sets none."""
        assert _targeted(tmp_path, {}, selector="tag:crm") == {
            "stg_customers",
            "stg_orders",
            "stg_raw_orders_for_snapshot",
        }

    def test_check_selector_replaces_the_global_one(self, tmp_path):
        """A check's own `selector` wins outright; it is not intersected with the global one."""
        assert _targeted(
            tmp_path, {"selector": "stg_payments"}, selector="tag:crm"
        ) == {"stg_payments"}

    def test_selected_models_are_the_ones_that_fail(self, tmp_path):
        """End to end, the failures are exactly the selected models."""
        config_file = _write_config(tmp_path, [{**_CHECK, "selector": "+orders"}])
        output_file = tmp_path / "results.json"

        exit_code = run_bouncer(config_file=config_file, output_file=output_file)

        results = json.loads(output_file.read_text(encoding="utf-8"))
        assert exit_code == ExitCode.CHECK_ERRORS
        assert {r["check_run_id"] for r in results if r["outcome"] == "failed"} == {
            "check_model_names:0:orders",
            "check_model_names:0:stg_orders",
            "check_model_names:0:stg_payments",
        }


class TestPathAndMaterializationFilters:
    """`include`, `exclude`, `materialization` and `selector` narrowing together."""

    @pytest.mark.parametrize(
        ("settings", "expected"),
        [
            pytest.param({"include": "^models/staging"}, _STAGING, id="include"),
            pytest.param(
                {"include": "^models/staging", "exclude": r".*stg_orders\.sql"},
                _STAGING - {"stg_orders"},
                id="exclude_removes_from_include",
            ),
            pytest.param(
                {"include": "^models/staging", "exclude": "^models/staging"},
                set(),
                id="exclude_beats_include",
            ),
            pytest.param(
                {
                    "include": "^models/staging",
                    "exclude": [r".*stg_orders\.sql", ".*/payments/"],
                },
                {"stg_customers", "stg_raw_orders_for_snapshot"},
                id="exclude_list_is_a_union",
            ),
            pytest.param(
                {"include": ["^models/marts", "^models/utilities/py_"]},
                {"orders", "py_order_stats", *_CUSTOMERS},
                id="include_list_is_a_union",
            ),
            pytest.param(
                {"materialization": "incremental"},
                {"int_incremental"},
                id="materialization",
            ),
            pytest.param(
                {"include": "^models/staging", "materialization": "view"},
                {"stg_customers", "stg_orders", "stg_payments"},
                id="materialization_within_include",
            ),
            pytest.param(
                {"include": "^models/marts", "materialization": "view"},
                set(),
                id="materialization_matching_nothing_in_include",
            ),
            pytest.param(
                {
                    "exclude": r".*stg_orders\.sql",
                    "include": "^models/staging",
                    "materialization": "view",
                    "selector": "stg_orders+ stg_customers",
                },
                {"stg_customers"},
                id="all_four_intersect",
            ),
        ],
    )
    def test_filters_combine(self, expected, settings, tmp_path):
        """Every filter set on a check must hold for a model to be targeted."""
        assert _targeted(tmp_path, settings) == expected

    def test_patterns_are_anchored_at_the_start_of_the_path(self, tmp_path):
        """A bare file name does not match: patterns are applied with `re.match`.

        `exclude: stg_orders` excludes nothing, because no path starts with it.
        Pinned so a switch to `re.search` is a deliberate, visible change.
        """
        assert (
            _targeted(tmp_path, {"include": "^models/staging", "exclude": "stg_orders"})
            == _STAGING
        )
        assert _targeted(tmp_path, {"include": "stg_orders"}) == set()

    def test_global_include_and_exclude_apply_to_checks_without_their_own(
        self, tmp_path
    ):
        """Top-level `include`/`exclude` are copied onto a check that omits them."""
        assert _targeted(
            tmp_path, {}, include="^models/marts", exclude=r".*customers_v[23]\.sql"
        ) == {"orders", "customers.v1"}

    def test_check_include_replaces_the_global_one(self, tmp_path):
        """A check's own `include` wins over the top-level one."""
        assert _targeted(
            tmp_path, {"include": "^models/staging/payments"}, include="^models/marts"
        ) == {"stg_payments"}


class TestInvalidSelector:
    """A malformed selector is a config error with a one-line message."""

    _CASES = pytest.mark.parametrize(
        ("selector", "message"),
        [
            pytest.param(
                "state:modified",
                "Invalid selector atom: 'state:modified'. Supported methods:",
                id="unsupported_method",
            ),
            pytest.param("tag:", "Invalid selector atom: 'tag:'.", id="empty_value"),
            pytest.param("+", "Invalid selector atom: '+'.", id="operator_only"),
            pytest.param(
                "config.:table",
                "Invalid selector atom: 'config.:table'.",
                id="empty_config_key",
            ),
            pytest.param(",", "Invalid selector: ','.", id="no_atoms"),
        ],
    )

    @_CASES
    @pytest.mark.parametrize("level", ["check", "global"])
    def test_run_bouncer_raises_config_error(self, level, message, selector, tmp_path):
        """`run_bouncer` raises `DbtBouncerConfigError` before reading any artifact."""
        if level == "check":
            config_file = _write_config(tmp_path, [{**_CHECK, "selector": selector}])
        else:
            config_file = _write_config(tmp_path, [_CHECK], selector=selector)

        with pytest.raises(DbtBouncerConfigError) as excinfo:
            run_bouncer(config_file=config_file)

        # The message is readable on its own: it quotes the bad selector.
        assert message in str(excinfo.value)

    @_CASES
    def test_cli_exits_config_error_without_traceback(
        self, caplog, message, selector, tmp_path
    ):
        """The CLI exits CONFIG_ERROR and logs the message, not a stack trace."""
        config_file = _write_config(tmp_path, [{**_CHECK, "selector": selector}])

        result = CliRunner().invoke(app, ["run", "--config-file", str(config_file)])

        assert result.exit_code == ExitCode.CONFIG_ERROR, result.output
        # `typer.Exit` surfaces as `SystemExit`; anything else is an uncaught error.
        assert isinstance(result.exception, SystemExit)
        assert message in caplog.text
        assert "Traceback" not in result.output
        assert "Traceback" not in caplog.text
