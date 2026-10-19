"""Tests for which resources a check targets, resolved against a real manifest.

`test_selectors.py` and the `resource_in_path` / `_check_applies_to_resource`
tests in `test_runner.py` cover selectors, path patterns, language and
materialization one at a time, against hand-built objects. These tests load the committed dbt_112
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
    """`include`, `exclude`, `language`, `materialization` and `selector` together."""

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
                {"language": "python"}, {"py_order_stats"}, id="language_python"
            ),
            pytest.param(
                {"language": "sql"},
                _ALL_MODELS - {"py_order_stats"},
                id="language_sql",
            ),
            pytest.param(
                {"language": "python", "materialization": "view"},
                set(),
                id="language_and_materialization_intersect",
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


# Expected `(staging_unique_id, marts_unique_id)` per `iterate_over` resource type
# in the Windows-path fixture below.
_WINDOWS_RESOURCE_IDS: dict[str, tuple[str, str]] = {
    "catalog_node": ("model.pkg.stg_model", "model.pkg.mart_model"),
    "catalog_source": ("source.pkg.raw.stg_source", "source.pkg.raw.mart_source"),
    "exposure": ("exposure.pkg.stg_exposure", "exposure.pkg.mart_exposure"),
    "macro": ("macro.pkg.stg_macro", "macro.pkg.mart_macro"),
    "model": ("model.pkg.stg_model", "model.pkg.mart_model"),
    "run_result": ("model.pkg.stg_model", "model.pkg.mart_model"),
    "seed": ("seed.pkg.stg_seed", "seed.pkg.mart_seed"),
    "semantic_model": (
        "semantic_model.pkg.stg_semantic_model",
        "semantic_model.pkg.mart_semantic_model",
    ),
    "snapshot": ("snapshot.pkg.stg_snapshot", "snapshot.pkg.mart_snapshot"),
    "source": ("source.pkg.raw.stg_source", "source.pkg.raw.mart_source"),
    "test": ("test.pkg.stg_test", "test.pkg.mart_test"),
    "unit_test": ("unit_test.pkg.stg_model.stg_ut", "unit_test.pkg.mart_model.mart_ut"),
}


def _make_check_for_resource_type(resource_type: str):
    """Build a minimal `@check` class whose `iterate_over` is `resource_type`.

    Returns:
        type[BaseCheck]: A check class targeting `resource_type`.

    """
    from dbt_bouncer.check_framework.decorator import check

    namespace: dict = {}
    exec(  # ruff: ignore[exec-builtin]
        f"def check_{resource_type}_noop({resource_type}): pass",
        namespace,
    )
    return check(namespace[f"check_{resource_type}_noop"])


_CHECKS_BY_RESOURCE_TYPE = {
    rt: _make_check_for_resource_type(rt) for rt in _WINDOWS_RESOURCE_IDS
}


@pytest.fixture(scope="module")
def windows_artifacts_dir(tmp_path_factory) -> Path:
    r"""Write artifacts where every resource has a Windows-style `original_file_path`.

    Each resource type gets one staging resource at ``models\\staging\\x.sql`` and
    one marts resource at ``models\\marts\\y.sql``.

    Returns:
        Path: Directory containing ``manifest.json``, ``catalog.json``, and
        ``run_results.json``.

    """
    target = tmp_path_factory.mktemp("windows_target")
    stg_path = "models\\staging\\x.sql"
    mart_path = "models\\marts\\y.sql"

    def _entry(uid: str, name: str, path: str, **extra) -> dict:
        return {
            "config": {"meta": {}},
            "fqn": ["pkg", name],
            "name": name,
            "original_file_path": path,
            "package_name": "pkg",
            "tags": [],
            "unique_id": uid,
            **extra,
        }

    manifest = {
        "child_map": {},
        "exposures": {
            "exposure.pkg.stg_exposure": _entry(
                "exposure.pkg.stg_exposure", "stg_exposure", stg_path
            ),
            "exposure.pkg.mart_exposure": _entry(
                "exposure.pkg.mart_exposure", "mart_exposure", mart_path
            ),
        },
        "macros": {
            "macro.pkg.stg_macro": _entry("macro.pkg.stg_macro", "stg_macro", stg_path),
            "macro.pkg.mart_macro": _entry(
                "macro.pkg.mart_macro", "mart_macro", mart_path
            ),
        },
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "1.12.0",
            "project_name": "pkg",
        },
        "nodes": {
            "model.pkg.stg_model": _entry(
                "model.pkg.stg_model", "stg_model", stg_path, resource_type="model"
            ),
            "model.pkg.mart_model": _entry(
                "model.pkg.mart_model", "mart_model", mart_path, resource_type="model"
            ),
            "seed.pkg.stg_seed": _entry(
                "seed.pkg.stg_seed", "stg_seed", stg_path, resource_type="seed"
            ),
            "seed.pkg.mart_seed": _entry(
                "seed.pkg.mart_seed", "mart_seed", mart_path, resource_type="seed"
            ),
            "snapshot.pkg.stg_snapshot": _entry(
                "snapshot.pkg.stg_snapshot",
                "stg_snapshot",
                stg_path,
                resource_type="snapshot",
            ),
            "snapshot.pkg.mart_snapshot": _entry(
                "snapshot.pkg.mart_snapshot",
                "mart_snapshot",
                mart_path,
                resource_type="snapshot",
            ),
            "test.pkg.stg_test": _entry(
                "test.pkg.stg_test", "stg_test", stg_path, resource_type="test"
            ),
            "test.pkg.mart_test": _entry(
                "test.pkg.mart_test", "mart_test", mart_path, resource_type="test"
            ),
        },
        "parent_map": {},
        "semantic_models": {
            "semantic_model.pkg.stg_semantic_model": _entry(
                "semantic_model.pkg.stg_semantic_model",
                "stg_semantic_model",
                stg_path,
            ),
            "semantic_model.pkg.mart_semantic_model": _entry(
                "semantic_model.pkg.mart_semantic_model",
                "mart_semantic_model",
                mart_path,
            ),
        },
        "sources": {
            "source.pkg.raw.stg_source": _entry(
                "source.pkg.raw.stg_source",
                "stg_source",
                stg_path,
                source_name="raw",
            ),
            "source.pkg.raw.mart_source": _entry(
                "source.pkg.raw.mart_source",
                "mart_source",
                mart_path,
                source_name="raw",
            ),
        },
        "unit_tests": {
            "unit_test.pkg.stg_model.stg_ut": _entry(
                "unit_test.pkg.stg_model.stg_ut", "stg_ut", stg_path
            ),
            "unit_test.pkg.mart_model.mart_ut": _entry(
                "unit_test.pkg.mart_model.mart_ut", "mart_ut", mart_path
            ),
        },
    }
    catalog = {
        "metadata": {},
        "nodes": {
            "model.pkg.stg_model": {
                "columns": {},
                "metadata": {},
                "stats": {},
                "unique_id": "model.pkg.stg_model",
            },
            "model.pkg.mart_model": {
                "columns": {},
                "metadata": {},
                "stats": {},
                "unique_id": "model.pkg.mart_model",
            },
        },
        "sources": {
            "source.pkg.raw.stg_source": {
                "columns": {},
                "metadata": {},
                "stats": {},
                "unique_id": "source.pkg.raw.stg_source",
            },
            "source.pkg.raw.mart_source": {
                "columns": {},
                "metadata": {},
                "stats": {},
                "unique_id": "source.pkg.raw.mart_source",
            },
        },
    }
    run_results = {
        "metadata": {},
        "results": [
            {"status": "pass", "unique_id": "model.pkg.stg_model"},
            {"status": "pass", "unique_id": "model.pkg.mart_model"},
        ],
    }

    (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    (target / "catalog.json").write_text(json.dumps(catalog), encoding="utf-8")
    (target / "run_results.json").write_text(json.dumps(run_results), encoding="utf-8")
    return target


def _windows_targeted_ids(
    windows_artifacts_dir: Path,
    resource_type: str,
    *,
    preserve_raw_wrapper_paths: bool = False,
    **check_kwargs,
) -> set[str]:
    """Assemble checks for `resource_type` against the Windows-path fixture.

    Args:
        windows_artifacts_dir: Directory containing the Windows-path artifacts.
        resource_type: The `iterate_over` resource type to test.
        preserve_raw_wrapper_paths: When True, overwrite wrapper
            `original_file_path` attributes back to raw backslash strings so
            `_CheckMatcher.resources_for` must normalise them itself.
        **check_kwargs: Filter arguments (`include`, `exclude`, `selector`).

    Returns:
        set[str]: The unique IDs of the targeted resources.

    """
    from types import SimpleNamespace

    from dbt_bouncer.artifact_parsers.parser import parse_dbt_artifacts
    from dbt_bouncer.context import BouncerContext

    check_cls = _CHECKS_BY_RESOURCE_TYPE[resource_type]
    check_instance = check_cls(index=0, **check_kwargs)

    stub_conf = SimpleNamespace(
        catalog_checks=[check_instance],
        info_schema_checks=[],
        manifest_checks=[check_instance],
        package_name="pkg",
        run_results_checks=[check_instance],
    )
    parsed = parse_dbt_artifacts(stub_conf, windows_artifacts_dir)

    if preserve_raw_wrapper_paths:
        for collection in (
            parsed.catalog_nodes,
            parsed.catalog_sources,
            parsed.models,
            parsed.run_results,
            parsed.seeds,
            parsed.semantic_models,
            parsed.snapshots,
            parsed.sources,
            parsed.tests,
        ):
            for wrapper in collection:
                wrapper.original_file_path = wrapper.original_file_path.replace(
                    "/", "\\"
                )

    ctx = BouncerContext.model_construct(
        bouncer_config=SimpleNamespace(manifest_checks=[check_instance]),
        catalog_nodes=parsed.catalog_nodes,
        catalog_sources=parsed.catalog_sources,
        check_categories=["manifest_checks"],
        create_pr_comment_file=False,
        dry_run=False,
        exposures=parsed.exposures,
        info_schema=parsed.info_schema,
        macros=parsed.macros,
        manifest_obj=parsed.manifest_obj,
        models=parsed.models,
        output_file=None,
        output_format="json",
        output_only_failures=False,
        run_results=parsed.run_results,
        seeds=parsed.seeds,
        semantic_models=parsed.semantic_models,
        show_all_failures=False,
        snapshots=parsed.snapshots,
        sources=parsed.sources,
        tests=parsed.tests,
        unit_tests=parsed.unit_tests,
    )
    return {str(entry["unique_id"]) for entry in _assemble_checks_to_run(ctx)}


class TestWindowsPathNormalisation:
    r"""Windows paths (`models\\staging\\x.sql`) normalise to POSIX for targeting.

    Covers `include`, `exclude`, and `path:` selectors across every resource
    type (models, sources, unit tests, exposures, macros, seeds, snapshots,
    semantic models, tests, catalog nodes, catalog sources, and run results).
    """

    def test_covers_every_runner_resource_type(self, windows_artifacts_dir):
        """Every resource type in `_build_resource_map` is exercised by this suite."""
        from types import SimpleNamespace

        from dbt_bouncer.artifact_parsers.parser import parse_dbt_artifacts
        from dbt_bouncer.context import BouncerContext
        from dbt_bouncer.runner import _build_resource_map

        stub_conf = SimpleNamespace(
            catalog_checks=[],
            info_schema_checks=[],
            manifest_checks=[],
            package_name="pkg",
            run_results_checks=[],
        )
        parsed = parse_dbt_artifacts(stub_conf, windows_artifacts_dir)
        ctx = BouncerContext.model_construct(**parsed._asdict())
        expected_types = {key.removesuffix("s") for key in _build_resource_map(ctx)}

        assert set(_WINDOWS_RESOURCE_IDS) == expected_types

    @pytest.mark.parametrize("resource_type", sorted(_WINDOWS_RESOURCE_IDS))
    @pytest.mark.parametrize("preserve_raw_wrapper_paths", [False, True])
    def test_include_matches_windows_backslash_path(
        self, preserve_raw_wrapper_paths, resource_type, windows_artifacts_dir
    ):
        r"""`include: ^models/staging` matches `models\\staging\\x.sql`."""
        stg_id, _ = _WINDOWS_RESOURCE_IDS[resource_type]

        targeted = _windows_targeted_ids(
            windows_artifacts_dir,
            resource_type,
            preserve_raw_wrapper_paths=preserve_raw_wrapper_paths,
            include="^models/staging",
        )

        assert targeted == {stg_id}

    @pytest.mark.parametrize("resource_type", sorted(_WINDOWS_RESOURCE_IDS))
    @pytest.mark.parametrize("preserve_raw_wrapper_paths", [False, True])
    def test_exclude_matches_windows_backslash_path(
        self, preserve_raw_wrapper_paths, resource_type, windows_artifacts_dir
    ):
        r"""`exclude: ^models/staging` excludes `models\\staging\\x.sql`."""
        _, mart_id = _WINDOWS_RESOURCE_IDS[resource_type]

        targeted = _windows_targeted_ids(
            windows_artifacts_dir,
            resource_type,
            preserve_raw_wrapper_paths=preserve_raw_wrapper_paths,
            exclude="^models/staging",
        )

        assert targeted == {mart_id}

    @pytest.mark.parametrize("resource_type", sorted(_WINDOWS_RESOURCE_IDS))
    @pytest.mark.parametrize(
        "selector",
        [
            pytest.param("path:models/staging", id="directory"),
            pytest.param("path:models/staging/*", id="glob"),
            pytest.param("path:models/staging/x.sql", id="exact_file"),
        ],
    )
    def test_path_selector_matches_windows_backslash_path(
        self, resource_type, selector, windows_artifacts_dir
    ):
        r"""`path:` selectors with forward slashes match `models\\staging\\x.sql`."""
        stg_id, _ = _WINDOWS_RESOURCE_IDS[resource_type]

        targeted = _windows_targeted_ids(
            windows_artifacts_dir,
            resource_type,
            selector=selector,
        )

        assert targeted == {stg_id}
