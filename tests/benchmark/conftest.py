"""Fixtures for the performance benchmark suite.

Builds a single large synthetic dbt project once per session and exposes the
artifacts, config, and helpers for the benchmark tests. The top-level
``tests/conftest.py`` still applies here (nested), so its autouse
``_rebuild_all_check_models`` / ``_clear_module_caches`` fixtures run too.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable

import pytest
import yaml

from .synthetic_manifest import _env_model_count, write_artifacts

if TYPE_CHECKING:
    from dbt_bouncer.context import BouncerContext

_CONFIG_PATH = Path(__file__).parent / "benchmark-config.yml"

# Resolved model count, shared from the ``n_models`` fixture to the terminal
# summary hook (which can't request fixtures). A ``StashKey`` is the pytest-
# recommended way to attach data to ``config`` without risking attribute clashes.
_N_MODELS_KEY = pytest.StashKey[int]()

# Friendly labels for the summary table, keyed by benchmark test function name.
_COMPONENT_LABELS = {
    "test_parse_manifest": "Parse artifacts",
    "test_check_discovery": "Check discovery",
    "test_validate_conf": "Config assembly",
    "test_runner": "Runner",
    "test_runner_match": "Match & copy",
    "test_runner_execute": "Execute",
    "test_runner_report": "Report",
    "test_run_bouncer": "Full run (end-to-end)",
}

# Display order and nesting depth (0 = top-level, 1 = indented sub-component).
# The runner sub-phases are nested beneath the overall "Runner" row.
_SUMMARY_ROWS = [
    ("test_parse_manifest", 0),
    ("test_check_discovery", 0),
    ("test_validate_conf", 0),
    ("test_runner", 0),
    ("test_runner_match", 1),
    ("test_runner_execute", 1),
    ("test_runner_report", 1),
    ("test_run_bouncer", 0),
]


def _format_duration(seconds: float) -> str:
    """Render a duration in the most readable unit (s / ms / μs).

    Args:
        seconds: The duration in seconds.

    Returns:
        The formatted duration, e.g. ``"1.43 ms"``.
    """
    if seconds >= 1:
        return f"{seconds:.2f} s"
    if seconds >= 1e-3:
        return f"{seconds * 1e3:.2f} ms"
    return f"{seconds * 1e6:.1f} μs"


def _format_pct(seconds: float, baseline: float | None) -> str:
    """Render a component's share of the full run as a percentage.

    Uses whole numbers at or above 10% (e.g. ``"90%"``) and one decimal place
    below it (e.g. ``"1.2%"``).

    Args:
        seconds: The component's mean duration in seconds.
        baseline: The full-run mean to measure against, or ``None`` if unknown.

    Returns:
        The formatted percentage, or ``"—"`` when no baseline is available.
    """
    # Guard both an unknown baseline (None) and a zero mean; the latter would
    # otherwise raise ZeroDivisionError below.
    if baseline is None or baseline == 0:
        return "—"
    pct = seconds / baseline * 100
    return f"{pct:.0f}%" if pct >= 10 else f"{pct:.1f}%"


def _count_configured_checks() -> int:
    """Count the checks defined in ``benchmark-config.yml``.

    Returns:
        The total number of check entries across every ``*_checks`` category.
    """
    contents = yaml.safe_load(_CONFIG_PATH.read_text())
    return sum(
        len(v)
        for k, v in contents.items()
        if k.endswith("_checks") and isinstance(v, list)
    )


@pytest.hookimpl(trylast=True)
def pytest_terminal_summary(config) -> None:
    """Print a rich summary table of the benchmarked components.

    Renders after pytest-benchmark's own stats table: one row per code
    component with its mean run time (the runner sub-phases nested beneath the
    overall ``Runner`` row) and the scale the run executed at (models parsed,
    checks evaluated).

    Args:
        config: The pytest config, carrying ``_benchmarksession``.
    """
    benchmark_session = getattr(config, "_benchmarksession", None)
    benchmarks = list(getattr(benchmark_session, "benchmarks", []) or [])
    if not benchmarks:
        return

    from rich import box
    from rich.console import Console
    from rich.table import Table

    # Prefer the count actually used to build the artifacts (stashed by the
    # ``n_models`` fixture) so the label can't drift from the real run size.
    n_models = config.stash.get(_N_MODELS_KEY, None)
    if n_models is None:
        n_models = _env_model_count(5000)
    n_checks = _count_configured_checks()

    table = Table(
        title="[bold cyan]dbt-bouncer benchmark summary[/bold cyan]",
        title_justify="left",
        caption=f"{n_models:,} models · {n_checks} checks evaluated",
        caption_justify="left",
        box=box.ROUNDED,
        border_style="cyan",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Component", justify="left", style="cyan", no_wrap=True)
    table.add_column("Mean", justify="right")
    table.add_column("%", justify="right")

    by_name = {bench.name: bench for bench in benchmarks}
    spec_names = {name for name, _ in _SUMMARY_ROWS}

    # Percentages are each component's share of the full end-to-end run.
    full_run = by_name.get("test_run_bouncer")
    baseline = full_run.stats.mean if full_run else None

    # Ordered rows (spec entries present in this run), with the runner sub-phases
    # nested beneath the "Runner" row via tree glyphs.
    ordered = [(name, depth) for name, depth in _SUMMARY_ROWS if name in by_name]
    for i, (name, depth) in enumerate(ordered):
        label = _COMPONENT_LABELS.get(name, name)
        if depth > 0:
            is_last = i + 1 >= len(ordered) or ordered[i + 1][1] < depth
            label = f"  {'└─' if is_last else '├─'} {label}"
        # Rule off the end-to-end total from the per-component breakdown above it.
        if name == "test_run_bouncer":
            table.add_section()
        mean = by_name[name].stats.mean
        table.add_row(label, _format_duration(mean), _format_pct(mean, baseline))

    # Any benchmarks not covered by the ordered spec (defensive fallback).
    for bench in benchmarks:
        if bench.name in spec_names:
            continue
        label = _COMPONENT_LABELS.get(
            bench.name, bench.name.removeprefix("test_").replace("_", " ").capitalize()
        )
        table.add_row(
            label,
            _format_duration(bench.stats.mean),
            _format_pct(bench.stats.mean, baseline),
        )

    Console().print(table)


@pytest.fixture(scope="session")
def n_models(request) -> int:
    """Resolve the benchmark model count once and stash it on the config.

    Both the artifact build and the terminal summary need this number; reading
    the env var in each spot risks them drifting apart. Resolving it here (and
    stashing it for the summary hook, which can't request fixtures) keeps the
    displayed scale honest to what was actually built.
    """
    count = _env_model_count(5000)
    request.config.stash[_N_MODELS_KEY] = count
    return count


@pytest.fixture(scope="session")
def synthetic_artifacts_dir(tmp_path_factory, n_models) -> Path:
    """Build the synthetic manifest/catalog/run_results once and return its dir."""
    target = tmp_path_factory.mktemp("synthetic") / "target"
    write_artifacts(target, n_models=n_models)
    return target


@pytest.fixture(scope="session")
def benchmark_config_contents() -> dict:
    """Return the parsed contents of ``benchmark-config.yml``."""
    return yaml.safe_load(_CONFIG_PATH.read_text())


@pytest.fixture(scope="session")
def benchmark_conf(benchmark_config_contents) -> tuple[list[str], dict]:
    """Return ``(check_categories, config_file_contents)`` for ``validate_conf``."""
    contents = dict(benchmark_config_contents)
    check_categories = [
        k for k in contents if k.endswith("_checks") and contents.get(k)
    ]
    return check_categories, contents


@pytest.fixture(scope="session")
def benchmark_config_file(
    tmp_path_factory, synthetic_artifacts_dir, benchmark_config_contents
) -> Path:
    """Write a config file whose ``dbt_artifacts_dir`` points at the synthetic dir."""
    contents = dict(benchmark_config_contents)
    contents["dbt_artifacts_dir"] = str(synthetic_artifacts_dir.resolve())
    cfg_path = tmp_path_factory.mktemp("bench_cfg") / "dbt-bouncer.yml"
    cfg_path.write_text(yaml.safe_dump(contents, sort_keys=False))
    return cfg_path


@pytest.fixture(scope="session")
def runner_inputs(benchmark_conf, synthetic_artifacts_dir):
    """Validate config and parse artifacts once for the runner benchmark.

    Mirrors the ``run_bouncer`` preprocessing (per-check index + global
    include/exclude). ``runner`` deletes resource fields off its context, so a
    fresh context must be built per round from these shared, unmutated inputs.
    """
    from dbt_bouncer.artifact_parsers.parser import parse_dbt_artifacts
    from dbt_bouncer.configuration_file.validator import validate_conf

    check_categories, contents = benchmark_conf
    bouncer_config = validate_conf(
        check_categories=check_categories,
        config_file_contents=dict(contents),
        custom_checks_dir=None,
    )
    for category in check_categories:
        for idx, check_obj in enumerate(getattr(bouncer_config, category)):
            check_obj.index = idx
            if bouncer_config.include and not check_obj.include:
                check_obj.include = bouncer_config.include
            if bouncer_config.exclude and not check_obj.exclude:
                check_obj.exclude = bouncer_config.exclude

    artifacts = parse_dbt_artifacts(
        bouncer_config=bouncer_config, dbt_artifacts_dir=synthetic_artifacts_dir
    )
    return bouncer_config, check_categories, artifacts


# Function-scoped on purpose: ``runner()`` deletes resource fields off the
# context it's handed, so each benchmark round needs a fresh context. The
# expensive inputs (parsed artifacts, validated config) stay session-scoped via
# ``runner_inputs``; only the cheap context wrapper is rebuilt per round.
@pytest.fixture
def make_bouncer_context(runner_inputs) -> Callable[[], BouncerContext]:
    """Return a factory that builds a fresh ``BouncerContext`` (no re-parse)."""
    from dbt_bouncer.context import BouncerContext

    bouncer_config, check_categories, artifacts = runner_inputs

    def _make() -> BouncerContext:
        return BouncerContext.model_construct(
            bouncer_config=bouncer_config,
            catalog_nodes=artifacts.catalog_nodes,
            catalog_sources=artifacts.catalog_sources,
            check_categories=check_categories,
            create_pr_comment_file=False,
            dry_run=False,
            exposures=artifacts.exposures,
            macros=artifacts.macros,
            manifest_obj=artifacts.manifest_obj,
            models=artifacts.models,
            output_file=None,
            output_format="json",
            output_only_failures=False,
            run_results=artifacts.run_results,
            seeds=artifacts.seeds,
            semantic_models=artifacts.semantic_models,
            show_all_failures=False,
            snapshots=artifacts.snapshots,
            sources=artifacts.sources,
            tests=artifacts.tests,
            unit_tests=artifacts.unit_tests,
        )

    return _make
