"""Fixtures for the performance benchmark suite.

Builds a single large synthetic dbt project once per session and exposes the
artifacts, config, and helpers for the benchmark tests. The top-level
``tests/conftest.py`` still applies here (nested), so its autouse
``_rebuild_all_check_models`` / ``_clear_module_caches`` fixtures run too.
"""

from __future__ import annotations

import os
import statistics
import time
from contextlib import contextmanager, nullcontext, redirect_stderr, redirect_stdout
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterator

import pytest
import yaml
from tqdm import tqdm

from .synthetic_manifest import _env_model_count, write_artifacts

if TYPE_CHECKING:
    from dbt_bouncer.context import BouncerContext

_CONFIG_PATH = Path(__file__).parent / "benchmark-config.yml"

# Resolved model count, shared from the ``n_models`` fixture to the terminal
# summary hook (which can't request fixtures). A ``StashKey`` is the pytest-
# recommended way to attach data to ``config`` without risking attribute clashes.
_N_MODELS_KEY = pytest.StashKey[int]()

# Phase decomposition of one warm ``run_bouncer`` call, stashed by the
# ``run_bouncer_phase_decomposition`` fixture for the summary hook to render.
# Value is ``(phase_stats: dict[str, dict[str, float]], wall_stats: dict[str, float])``,
# each stats dict holding ``median`` / ``stdev`` / ``min`` / ``max`` in seconds.
_PHASE_KEY = pytest.StashKey[tuple]()

# Number of warm rounds measured for the phase decomposition (after the discarded
# warmup rounds below).
_PHASE_ROUNDS = 100

# Warmup rounds run (and discarded) before timing starts, so the disk conf cache
# and lru caches are hot for every measured round.
_WARMUP_ROUNDS = 1

# The discrete callables ``run_bouncer`` invokes that we time, mapped to a phase
# key. Each is a strict sub-interval of the full-run wall clock and none nests
# inside another here (``runner`` itself is *not* wrapped — it's reconstructed
# from its three sub-phases), so the timings never double count. Everything the
# run does that isn't one of these calls (config mutation, ``--check``/``--only``
# filtering, context construction, imports) falls into the ``Other`` residual.
# Resolved lazily inside ``_phase_timers`` so importing this module stays cheap.
_PHASE_TARGETS = [
    ("dbt_bouncer.configuration_file.validator", "get_config_file_path", "config_load"),
    (
        "dbt_bouncer.configuration_file.validator",
        "load_config_file_contents",
        "config_load",
    ),
    ("dbt_bouncer.configuration_file.validator", "validate_conf", "config_assembly"),
    ("dbt_bouncer.artifact_parsers.parser", "parse_dbt_artifacts", "parse"),
    ("dbt_bouncer.runner", "_assemble_checks_to_run", "match"),
    ("dbt_bouncer.executor:Executor", "run", "execute"),
    ("dbt_bouncer.reporting.reporter:Reporter", "report_results", "report"),
]

# Phase keys measured directly (the rest are computed: ``runner`` = sum of its
# sub-phases, ``other`` = residual).
_MEASURED_PHASES = (
    "config_load",
    "config_assembly",
    "parse",
    "match",
    "execute",
    "report",
)

# Display order and nesting depth (0 = top-level, 1 = indented sub-phase). The
# runner sub-phases nest beneath the overall "Runner" row. These rows' *means*
# would sum exactly to the full run (config_load + config_assembly + parse +
# runner + other == wall); the displayed medians only sum approximately, since
# medians aren't additive the way means are.
_PHASE_SUMMARY_ROWS = [
    ("config_load", "Config discovery + load", 0),
    ("config_assembly", "Config assembly (warm)", 0),
    ("parse", "Parse artifacts", 0),
    ("runner", "Runner", 0),
    ("match", "Match & copy", 1),
    ("execute", "Execute", 1),
    ("report", "Report", 1),
    ("other", "Other (glue / context build)", 0),
]


def _pick_unit(seconds: float) -> tuple[float, str]:
    """Pick a single duration unit for the whole table, sized to ``seconds``.

    Called once with the full-run median (the table's largest value) so every
    row renders in the same unit and stays vertically aligned.

    Args:
        seconds: The largest duration the table will render.

    Returns:
        ``(divisor, suffix)`` such that ``seconds * divisor`` is in that unit.
    """
    if seconds >= 1:
        return 1.0, "s"
    if seconds >= 1e-3:
        return 1e3, "ms"
    return 1e6, "μs"


def _format_duration(seconds: float, divisor: float, suffix: str) -> str:
    """Render a duration in the table's shared unit, fixed to 2 decimal places.

    Args:
        seconds: The duration in seconds.
        divisor: The multiplier (from ``_pick_unit``) converting seconds to the
            table's shared unit.
        suffix: The unit label (from ``_pick_unit``), e.g. ``"ms"``.

    Returns:
        The formatted duration, e.g. ``"6.70 ms"``.
    """
    return f"{seconds * divisor:.2f} {suffix}"


def _pct_value(seconds: float, baseline: float | None) -> float:
    """Compute a component's share of the full run as a percentage.

    Args:
        seconds: The component's median duration in seconds.
        baseline: The full-run median to measure against, or ``None``/``0`` if
            unknown.

    Returns:
        The percentage (0-100), or ``0.0`` when no baseline is available.
    """
    # Guard both an unknown baseline (None) and a zero median; the latter would
    # otherwise raise ZeroDivisionError below.
    if not baseline:
        return 0.0
    return seconds / baseline * 100


def _format_pct(pct: float) -> str:
    """Render a percentage at a fixed 1-decimal precision, e.g. ``"91.2%"``.

    A fixed precision (rather than switching decimal places by magnitude)
    keeps the ``%`` column aligned across rows.
    """
    return f"{pct:.1f}%"


# Floor for the terminal summary's console width (see ``pytest_terminal_summary``).
_MIN_TABLE_WIDTH = 120


def _series_stats(vals: list[float]) -> dict[str, float]:
    """Compute median/stdev/min/max for one phase's per-round samples.

    Args:
        vals: The per-round durations in seconds for one phase.

    Returns:
        A dict with ``median``, ``stdev``, ``min``, and ``max`` keys (seconds).
        ``stdev`` is the sample stdev, or ``0.0`` when fewer than 2 samples.
    """
    return {
        "median": statistics.median(vals),
        "stdev": statistics.stdev(vals) if len(vals) > 1 else 0.0,
        "min": min(vals),
        "max": max(vals),
    }


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


@contextmanager
def _phase_timers(accumulator: dict[str, float]) -> Iterator[None]:
    """Time the callables ``run_bouncer`` invokes, accumulating per phase.

    Swaps each target in ``_PHASE_TARGETS`` for a wrapper that adds its elapsed
    time to ``accumulator[phase]``, then restores every original on exit. The
    run's local imports (``from ... import validate_conf`` etc.) resolve the
    patched attribute at call time, so patching the source module/class here is
    enough to intercept the composed run. Method targets receive ``self`` as the
    first positional arg naturally.

    Not thread-safe: the targets are patched in place as global module/class
    attributes, so this assumes a single-threaded run (which the benchmark suite
    is). Running it under parallel workers (e.g. ``pytest-xdist``) that share a
    process would let concurrent runs clobber each other's patches.

    Args:
        accumulator: Mutable mapping of phase key -> accumulated seconds.

    Yields:
        None, with the timers installed for the duration of the ``with`` block.
    """
    import importlib

    def _resolve(dotted: str) -> tuple[object, str]:
        # "pkg.mod:Class" -> patch the class attribute; "pkg.mod" -> module attr.
        module_path, _, cls_name = dotted.partition(":")
        owner: object = importlib.import_module(module_path)
        if cls_name:
            owner = getattr(owner, cls_name)
        return owner, cls_name

    def _make_wrapper(orig, phase):
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            try:
                return orig(*args, **kwargs)
            finally:
                accumulator[phase] = accumulator.get(phase, 0.0) + (
                    time.perf_counter() - start
                )

        return wrapper

    originals: list[tuple[object, str, object]] = []
    for dotted, attr, phase in _PHASE_TARGETS:
        owner, _ = _resolve(dotted)
        orig = getattr(owner, attr)
        originals.append((owner, attr, orig))
        setattr(owner, attr, _make_wrapper(orig, phase))
    try:
        yield
    finally:
        for owner, attr, orig in originals:
            setattr(owner, attr, orig)


@pytest.hookimpl(trylast=True)
def pytest_terminal_summary(config) -> None:
    """Print a rich summary table decomposing one warm end-to-end run.

    Renders after pytest-benchmark's own stats table. Rows come from
    ``run_bouncer_phase_decomposition`` (stashed on the config): each phase's
    median time (with stdev/min/max) as a share of the full-run wall time, with
    the runner sub-phases nested beneath the overall ``Runner`` row and an
    ``Other`` residual so the breakdown approximately sums to the full run
    (see the fixture's docstring — medians aren't additive, so this isn't
    exact). Every duration in the table shares one unit (picked from the
    full-run median) and every percentage uses a fixed 1-decimal precision, so
    columns stay aligned. The caption reports the scale the run executed at
    (models parsed, checks evaluated).

    Args:
        config: The pytest config, carrying the stashed phase decomposition.
    """
    decomposition = config.stash.get(_PHASE_KEY, None)
    if decomposition is None:
        # No full run in this session (e.g. a filtered selection) — nothing to
        # decompose. pytest-benchmark's own stats table still prints above.
        return
    phase_stats, wall_stats = decomposition

    from rich import box
    from rich.console import Console
    from rich.table import Table

    # Prefer the count actually used to build the artifacts (stashed by the
    # ``n_models`` fixture) so the label can't drift from the real run size.
    n_models = config.stash.get(_N_MODELS_KEY, None)
    if n_models is None:
        n_models = _env_model_count(5000)
    n_checks = _count_configured_checks()

    divisor, unit = _pick_unit(wall_stats["median"])
    warmup_label = "warm-up" if _WARMUP_ROUNDS == 1 else "warm-ups"

    # Rich falls back to an 80-column console when stdout isn't a real TTY
    # (piped output, CI logs) — too narrow for this table's 6 columns, which
    # would otherwise wrap every cell onto two lines. Force a floor wide enough
    # for the table's natural width; a real, wider terminal is unaffected since
    # this only raises the wrap threshold, it doesn't stretch the table to fit.
    console = Console(width=max(Console().size.width, _MIN_TABLE_WIDTH))

    table = Table(
        title=(
            "[bold cyan]dbt-bouncer Benchmark Summary[/bold cyan] "
            f"(n={_PHASE_ROUNDS} runs, {_WARMUP_ROUNDS} {warmup_label} dropped)"
        ),
        title_justify="left",
        caption=f"{n_models:,} models · {n_checks} checks evaluated",
        caption_justify="left",
        box=box.ROUNDED,
        border_style="cyan",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Component", justify="left", style="cyan", no_wrap=True)
    table.add_column("Median", justify="right")
    table.add_column("StdDev", justify="right")
    table.add_column("Min", justify="right")
    table.add_column("Max", justify="right")
    table.add_column("%", justify="right")

    def _row(label: str, stats: dict[str, float], pct: float) -> None:
        table.add_row(
            label,
            _format_duration(stats["median"], divisor, unit),
            f"±{_format_duration(stats['stdev'], divisor, unit)}",
            _format_duration(stats["min"], divisor, unit),
            _format_duration(stats["max"], divisor, unit),
            _format_pct(pct),
        )

    # Per-phase rows, with the runner sub-phases nested beneath "Runner" via tree
    # glyphs (a sub-phase gets └─ when the next row steps back out to depth 0).
    for i, (key, label, depth) in enumerate(_PHASE_SUMMARY_ROWS):
        if depth > 0:
            nxt = (
                _PHASE_SUMMARY_ROWS[i + 1] if i + 1 < len(_PHASE_SUMMARY_ROWS) else None
            )
            is_last = nxt is None or nxt[2] < depth
            label = f"  {'└─' if is_last else '├─'} {label}"
        stats = phase_stats[key]
        pct = _pct_value(stats["median"], wall_stats["median"])
        _row(label, stats, pct)

    # Rule off the end-to-end total from the per-phase breakdown above it.
    table.add_section()
    _row("Full run (end-to-end)", wall_stats, 100.0)

    console.print(table)


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
def run_bouncer_phase_decomposition(request, benchmark_config_file) -> tuple:
    """Time one warm ``run_bouncer`` broken into phases for the summary table.

    Wraps the discrete callables the run invokes (see ``_PHASE_TARGETS``) and
    accumulates their elapsed time per phase over ``_PHASE_ROUNDS`` warm rounds
    (``_WARMUP_ROUNDS`` rounds are discarded first so the disk conf cache and
    lru caches are hot, matching what ``test_run_bouncer`` measures). Because
    the wrapped calls are strict sub-intervals of the measured wall clock,
    ``sum(phases) <= wall`` for every round.

    ``runner`` and ``other`` aren't measured directly: ``runner`` is the
    per-round sum of its three sub-phases (match/execute/report), and ``other``
    is the per-round residual (``wall - accounted``, clamped at 0). Both get
    genuine per-round series, so every row's median/stdev/min/max come from the
    same underlying samples (``min <= median <= max`` holds for every row).
    Unlike the old mean-only version, this means the rendered rows no longer
    sum to the full-run row *exactly* — medians aren't additive the way means
    are — but in practice they're close, since ``other`` is a small residual.

    Each round is a full end-to-end run, so ``_PHASE_ROUNDS`` rounds can take a
    while; a ``tqdm`` progress bar reports round-by-round progress. It suspends
    pytest's own output capturing for just this fixture (via the
    ``capturemanager`` plugin) so it's visible without needing ``pytest -s`` —
    which would also un-suppress every *other* benchmark's own dbt-bouncer
    output, since those call the real functions directly, many times over, via
    pytest-benchmark's calibration/measurement rounds.

    Stashes and returns ``(phase_stats, wall_stats)`` for the terminal summary
    hook (which can't request fixtures).

    Returns:
        A tuple of ``(phase_stats, wall_stats)``: per-phase stats dicts
        (``median``/``stdev``/``min``/``max`` in seconds, including computed
        ``runner`` and ``other`` keys) and the full-run wall-time stats dict.
    """
    from dbt_bouncer.cli.run.utils import run_bouncer

    def target() -> int:
        # ``verbosity=0`` still logs at INFO (to stderr, via a plain
        # ``logging.StreamHandler()``) and the executor/reporter print their own
        # rich tables unconditionally (to stdout). Over ``_PHASE_ROUNDS`` rounds
        # that would bury the tqdm progress bar below, so silence both streams
        # here — only for the duration of this call, so tqdm's own redraw
        # between rounds is unaffected.
        with (
            Path(os.devnull).open("w") as devnull,
            redirect_stdout(devnull),
            redirect_stderr(devnull),
        ):
            return run_bouncer(config_file=benchmark_config_file, verbosity=0)

    # Suspend pytest's global capture just for this fixture so the tqdm bars
    # below reach the real terminal, without needing ``pytest -s`` (which would
    # also unmute every other benchmark's own dbt-bouncer output).
    capmanager = request.config.pluginmanager.getplugin("capturemanager")
    show_progress = (
        capmanager.global_and_fixture_disabled if capmanager else nullcontext
    )

    with show_progress():
        for _ in tqdm(range(_WARMUP_ROUNDS), desc="Warming up caches", leave=False):
            target()  # warm caches; discard

        per_phase: dict[str, list[float]] = {k: [] for k in _MEASURED_PHASES}
        walls: list[float] = []
        for _ in tqdm(range(_PHASE_ROUNDS), desc="Benchmarking dbt-bouncer"):
            acc: dict[str, float] = {}
            start = time.perf_counter()
            with _phase_timers(acc):
                target()
            walls.append(time.perf_counter() - start)
            for key in _MEASURED_PHASES:
                per_phase[key].append(acc.get(key, 0.0))

    runner_series = [
        per_phase["match"][i] + per_phase["execute"][i] + per_phase["report"][i]
        for i in range(_PHASE_ROUNDS)
    ]
    accounted_series = [
        per_phase["config_load"][i]
        + per_phase["config_assembly"][i]
        + per_phase["parse"][i]
        + runner_series[i]
        for i in range(_PHASE_ROUNDS)
    ]
    other_series = [
        max(0.0, walls[i] - accounted_series[i]) for i in range(_PHASE_ROUNDS)
    ]

    phase_stats: dict[str, dict[str, float]] = {
        key: _series_stats(vals) for key, vals in per_phase.items()
    }
    phase_stats["runner"] = _series_stats(runner_series)
    phase_stats["other"] = _series_stats(other_series)

    wall_stats = _series_stats(walls)

    decomposition = (phase_stats, wall_stats)
    request.config.stash[_PHASE_KEY] = decomposition
    return decomposition


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
