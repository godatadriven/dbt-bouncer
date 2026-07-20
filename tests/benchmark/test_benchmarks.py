"""Performance benchmarks for dbt-bouncer's hot paths.

Run via ``make test-benchmark`` (which omits ``--numprocesses`` — pytest-benchmark
disables itself under xdist — and ``--cov`` — coverage skews timings). Uses the
``benchmark`` fixture from pytest-benchmark; the JSON it emits is tracked by
Bencher (``--adapter python_pytest``) so regressions fail PRs.

Targets:
- ``test_parse_manifest``  -> parse-time (``parse_dbt_artifacts``).
- ``test_validate_conf``   -> check-assembly (config discovery + discriminated
  union build + Pydantic validation), measured cold.
- ``test_check_discovery`` -> check-class discovery only.
- ``test_runner``          -> full runner: assemble + execute + report.
- ``test_runner_match``    -> runner sub-phase: match + ``model_copy(deep=True)``.
- ``test_runner_execute``  -> runner sub-phase: threaded check execution.
- ``test_runner_report``   -> runner sub-phase: result formatting + output.
- ``test_run_bouncer``     -> full in-process end-to-end run.
"""

from __future__ import annotations

from dbt_bouncer.artifact_parsers.parser import parse_dbt_artifacts
from dbt_bouncer.configuration_file.parser import DbtBouncerConfBase
from dbt_bouncer.configuration_file.validator import validate_conf
from dbt_bouncer.executor import Executor
from dbt_bouncer.reporting.reporter import Reporter
from dbt_bouncer.runner import _assemble_checks_to_run, runner
from dbt_bouncer.utils import get_check_objects


def _clear_assembly_caches() -> None:
    """Clear the memoisation caches that would otherwise warm the cold path."""
    import dbt_bouncer.configuration_file.parser as parser_mod
    import dbt_bouncer.utils as utils_mod

    candidates = [
        getattr(utils_mod, "get_check_objects", None),
        getattr(utils_mod, "get_check_objects_for_names", None),
        getattr(utils_mod, "get_check_registry", None),
        getattr(utils_mod, "_internal_checks_digest", None),
        getattr(parser_mod, "create_bouncer_conf_class", None),
        getattr(parser_mod, "_create_conf_class", None),
    ]
    for fn in candidates:
        if fn is not None and hasattr(fn, "cache_clear"):
            fn.cache_clear()


def test_parse_manifest(benchmark, synthetic_artifacts_dir):
    """Benchmark parsing the synthetic manifest into proxy objects."""
    config = DbtBouncerConfBase()
    result = benchmark(
        parse_dbt_artifacts,
        bouncer_config=config,
        dbt_artifacts_dir=synthetic_artifacts_dir,
    )
    assert len(result.models) > 0


def test_check_discovery(benchmark):
    """Benchmark discovering and importing all check classes (cold)."""

    def setup():
        _clear_assembly_caches()
        return (), {}

    checks = benchmark.pedantic(get_check_objects, setup=setup, rounds=5, iterations=1)
    assert len(checks) > 0


def test_validate_conf(benchmark, monkeypatch, benchmark_conf):
    """Benchmark check-assembly: config validation on the cold path."""
    # ``monkeypatch`` is function-scoped and only sets an env var here — it never
    # mutates the session-scoped ``benchmark_conf``, so mixing the two scopes is
    # safe. Setting it for the whole test (not just setup) also keeps the
    # benchmark-timed ``validate_conf`` call on the cold path, never the cache.
    monkeypatch.setenv("DBT_BOUNCER_DISABLE_CONF_CACHE", "1")
    check_categories, contents = benchmark_conf

    def setup():
        _clear_assembly_caches()
        return (), {
            "check_categories": check_categories,
            "config_file_contents": dict(contents),
            "custom_checks_dir": None,
        }

    result = benchmark.pedantic(validate_conf, setup=setup, rounds=5, iterations=1)
    assert result is not None


def test_runner(benchmark, make_bouncer_context):
    """Benchmark the runner: matching checks to nodes, copying, and executing.

    ``runner`` deletes resource fields off its context, so each round gets a
    freshly built context (from pre-parsed artifacts, no re-parse).
    """

    def setup():
        return (), {"ctx": make_bouncer_context()}

    exit_code, _ = benchmark.pedantic(runner, setup=setup, rounds=8, iterations=1)
    assert exit_code in (0, 1)


def test_runner_match(benchmark, make_bouncer_context):
    """Benchmark the runner's match phase: matching + ``model_copy()`` (shallow).

    Assembles the ``checks_to_run`` list without executing or reporting. A fresh
    context is built per round because ``_assemble_checks_to_run`` reads resource
    fields that the full ``runner`` later deletes.
    """

    def setup():
        return (), {"ctx": make_bouncer_context()}

    checks_to_run = benchmark.pedantic(
        _assemble_checks_to_run, setup=setup, rounds=8, iterations=1
    )
    assert len(checks_to_run) > 0


def test_runner_execute(benchmark, make_bouncer_context):
    """Benchmark the runner's execute phase: threaded check execution.

    ``setup`` assembles a fresh ``checks_to_run`` each round (not timed by
    pytest-benchmark); only ``Executor.run`` is timed.
    """
    executor = Executor()

    def setup():
        checks_to_run = _assemble_checks_to_run(make_bouncer_context())
        return (checks_to_run,), {}

    results = benchmark.pedantic(executor.run, setup=setup, rounds=5, iterations=1)
    assert len(results) > 0


def test_runner_report(benchmark, make_bouncer_context):
    """Benchmark the runner's report phase: formatting results and output.

    ``setup`` runs assemble + execute each round (not timed) to produce the
    ``results`` list; only ``Reporter.report_results`` is timed. The reporter is
    constructed with the same flags the full ``runner`` uses.
    """
    reporter = Reporter(
        show_all_failures=False,
        create_pr_comment_file=False,
        output_file=None,
        output_format="json",
        output_only_failures=False,
    )

    def setup():
        checks_to_run = _assemble_checks_to_run(make_bouncer_context())
        results = Executor().run(checks_to_run)
        return (results,), {}

    exit_code, _ = benchmark.pedantic(
        reporter.report_results, setup=setup, rounds=5, iterations=1
    )
    assert exit_code in (0, 1)


def test_run_bouncer(benchmark, benchmark_config_file, run_bouncer_phase_decomposition):
    """Benchmark a full in-process run over the synthetic manifest.

    Depends on ``run_bouncer_phase_decomposition`` so the per-phase breakdown is
    measured and stashed (for the terminal summary table) *before* this clean,
    unwrapped ``benchmark(...)`` call produces the headline full-run timing.
    """
    # The decomposition must account for the whole run: the top-level phases plus
    # the ``Other`` residual sum to the wall time (this is the invariant that lets
    # the summary table sum to 100%).
    phase_means, wall_mean = run_bouncer_phase_decomposition
    top_level = ("config_load", "config_assembly", "parse", "runner", "other")
    # ``other`` is defined as ``wall - accounted``, so this sum equals ``wall_mean``
    # exactly by construction — it's an arithmetic invariant, not a measurement one.
    # The tiny ``1e-6`` tolerance only absorbs floating-point rounding, so it never
    # flakes on timing variance.
    assert abs(sum(phase_means[k] for k in top_level) - wall_mean) <= wall_mean * 1e-6

    from dbt_bouncer.cli.run.utils import run_bouncer

    exit_code = benchmark(run_bouncer, config_file=benchmark_config_file, verbosity=0)
    assert exit_code in (0, 1)
