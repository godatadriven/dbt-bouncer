"""Fixtures for the performance benchmark suite.

Builds a single large synthetic dbt project once per session and exposes the
artifacts, config, and helpers for the benchmark tests. The top-level
``tests/conftest.py`` still applies here (nested), so its autouse
``_rebuild_all_check_models`` / ``_clear_module_caches`` fixtures run too.
"""

from __future__ import annotations

import time
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Iterator

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

# Phase decomposition of one warm ``run_bouncer`` call, stashed by the
# ``run_bouncer_phase_decomposition`` fixture for the summary hook to render.
# Value is ``(phase_means: dict[str, float], wall_mean: float)``.
_PHASE_KEY = pytest.StashKey[tuple]()

# Number of warm rounds averaged for the phase decomposition (after a discarded
# warmup round). Kept small — each round is a full end-to-end run.
_PHASE_ROUNDS = 5

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
# runner sub-phases nest beneath the overall "Runner" row. These rows sum to the
# full run by construction: config_load + config_assembly + parse + runner +
# other == wall.
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


@contextmanager
def _phase_timers(accumulator: dict[str, float]) -> Iterator[None]:
    """Time the callables ``run_bouncer`` invokes, accumulating per phase.

    Swaps each target in ``_PHASE_TARGETS`` for a wrapper that adds its elapsed
    time to ``accumulator[phase]``, then restores every original on exit. The
    run's local imports (``from ... import validate_conf`` etc.) resolve the
    patched attribute at call time, so patching the source module/class here is
    enough to intercept the composed run. Method targets receive ``self`` as the
    first positional arg naturally.

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
    mean time as a share of the full-run wall time, with the runner sub-phases
    nested beneath the overall ``Runner`` row and an ``Other`` residual so the
    breakdown sums to the full run exactly. The caption reports the scale the run
    executed at (models parsed, checks evaluated).

    Args:
        config: The pytest config, carrying the stashed phase decomposition.
    """
    decomposition = config.stash.get(_PHASE_KEY, None)
    if decomposition is None:
        # No full run in this session (e.g. a filtered selection) — nothing to
        # decompose. pytest-benchmark's own stats table still prints above.
        return
    phase_means, wall_mean = decomposition

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

    # Per-phase rows, with the runner sub-phases nested beneath "Runner" via tree
    # glyphs (a sub-phase gets └─ when the next row steps back out to depth 0).
    for i, (key, label, depth) in enumerate(_PHASE_SUMMARY_ROWS):
        if depth > 0:
            nxt = (
                _PHASE_SUMMARY_ROWS[i + 1] if i + 1 < len(_PHASE_SUMMARY_ROWS) else None
            )
            is_last = nxt is None or nxt[2] < depth
            label = f"  {'└─' if is_last else '├─'} {label}"
        mean = phase_means.get(key, 0.0)
        table.add_row(label, _format_duration(mean), _format_pct(mean, wall_mean))

    # Rule off the end-to-end total from the per-phase breakdown above it.
    table.add_section()
    table.add_row(
        "Full run (end-to-end)",
        _format_duration(wall_mean),
        _format_pct(wall_mean, wall_mean),
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
def run_bouncer_phase_decomposition(request, benchmark_config_file) -> tuple:
    """Time one warm ``run_bouncer`` broken into phases for the summary table.

    Wraps the discrete callables the run invokes (see ``_PHASE_TARGETS``) and
    accumulates their elapsed time per phase over ``_PHASE_ROUNDS`` warm rounds
    (a warmup round is discarded so the disk conf cache and lru caches are hot,
    matching what ``test_run_bouncer`` measures). Because the wrapped calls are
    strict sub-intervals of the measured wall clock, ``sum(phases) <= wall`` and
    the ``Other`` residual is always non-negative, so the rendered breakdown sums
    to the full run exactly.

    Stashes and returns ``(phase_means, wall_mean)`` for the terminal summary
    hook (which can't request fixtures).

    Returns:
        A tuple of the per-phase mean seconds (including computed ``runner`` and
        ``other`` keys) and the full-run wall-time mean in seconds.
    """
    from statistics import mean

    from dbt_bouncer.cli.run.utils import run_bouncer

    def target() -> int:
        return run_bouncer(config_file=benchmark_config_file, verbosity=0)

    target()  # warm caches; discard

    per_phase: dict[str, list[float]] = {k: [] for k in _MEASURED_PHASES}
    walls: list[float] = []
    for _ in range(_PHASE_ROUNDS):
        acc: dict[str, float] = {}
        start = time.perf_counter()
        with _phase_timers(acc):
            target()
        walls.append(time.perf_counter() - start)
        for key in _MEASURED_PHASES:
            per_phase[key].append(acc.get(key, 0.0))

    phase_means: dict[str, float] = {key: mean(vals) for key, vals in per_phase.items()}
    phase_means["runner"] = (
        phase_means["match"] + phase_means["execute"] + phase_means["report"]
    )
    wall_mean = mean(walls)
    accounted = (
        phase_means["config_load"]
        + phase_means["config_assembly"]
        + phase_means["parse"]
        + phase_means["runner"]
    )
    phase_means["other"] = max(0.0, wall_mean - accounted)

    decomposition = (phase_means, wall_mean)
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
