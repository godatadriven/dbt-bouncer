"""Sweep the end-to-end benchmark across model counts and print a summary table.

Runs the ``test_run_bouncer`` benchmark (a full in-process ``run_bouncer`` over a
synthetic manifest) once per model count and reports the mean run time for each,
so you can see how dbt-bouncer scales with project size.

Each model count needs its own ``pytest`` process because the synthetic manifest
is built by a session-scoped fixture (see ``tests/benchmark/conftest.py``), so the
count can only be set once per run via ``DBT_BOUNCER_BENCH_MODELS``.

Run via mise (recommended)::

    mise run test-benchmark-aggregate
    mise run test-benchmark-aggregate --model-counts "100 1000 10000"

or directly::

    uv run python tests/benchmark/aggregate_benchmarks.py --models "100,250,500"
"""

from __future__ import annotations

import json
import os
import subprocess  # noqa: S404 - fixed, fully-controlled command (no shell, no user input)
import sys
import tempfile
from pathlib import Path
from typing import Annotated

import typer

# The end-to-end benchmark whose mean we report as "total run time".
_FULL_RUN_BENCHMARK = "test_run_bouncer"

# Model counts swept when none are supplied on the command line.
DEFAULT_MODEL_COUNTS = [100, 250, 500, 1000, 2000, 5000, 10000]

_REPO_ROOT = Path(__file__).resolve().parents[2]


def _parse_model_counts(raw: str | None) -> list[int]:
    """Parse a space- or comma-separated model-count string into ints.

    Args:
        raw: The raw ``--models`` value, or ``None`` to use the defaults.

    Returns:
        The parsed, positive model counts (defaults when ``raw`` is empty).

    Raises:
        typer.BadParameter: If a token is not a valid integer.
    """
    if not raw or not raw.strip():
        return list(DEFAULT_MODEL_COUNTS)
    counts = []
    for token in raw.replace(",", " ").split():
        try:
            counts.append(max(1, int(token)))
        except ValueError as exc:
            raise typer.BadParameter(f"Invalid model count: {token!r}") from exc
    return counts


def _parse_mean(data: dict) -> float:
    """Return the mean run time (seconds) of the full-run benchmark.

    Args:
        data: The parsed contents of a pytest-benchmark JSON report.

    Returns:
        The ``stats.mean`` of the ``test_run_bouncer`` benchmark, in seconds.

    Raises:
        KeyError: If the report has no ``test_run_bouncer`` benchmark entry.
    """
    for bench in data.get("benchmarks", []):
        if bench.get("name") == _FULL_RUN_BENCHMARK:
            return float(bench["stats"]["mean"])
    found = [bench.get("name") for bench in data.get("benchmarks", [])]
    raise KeyError(
        f"No {_FULL_RUN_BENCHMARK!r} benchmark found in results (got: {found})"
    )


def _format_seconds(seconds: float | None) -> str:
    """Render a duration in whole seconds with fixed precision.

    Args:
        seconds: The duration in seconds, or ``None`` when the run failed.

    Returns:
        The duration to four decimal places, or ``"—"`` when unknown.
    """
    if seconds is None:
        return "—"
    return f"{seconds:.4f}"


def _build_table(results: list[tuple[int, float | None]]):
    """Build the aggregate summary table.

    Args:
        results: ``(model_count, mean_seconds_or_None)`` pairs in display order.

    Returns:
        A populated ``rich.table.Table``.
    """
    from rich import box
    from rich.table import Table

    table = Table(
        title="[bold cyan]dbt-bouncer benchmark aggregate[/bold cyan]",
        title_justify="left",
        caption="mean of the end-to-end run (test_run_bouncer) per model count",
        caption_justify="left",
        box=box.ROUNDED,
        border_style="cyan",
        show_header=True,
        header_style="bold cyan",
    )
    table.add_column("Number of models", justify="right", style="cyan", no_wrap=True)
    table.add_column("Total run time (s)", justify="right")
    for count, mean in results:
        table.add_row(f"{count:,}", _format_seconds(mean))
    return table


def _run_single(n_models: int) -> float | None:
    """Run the full-run benchmark once at ``n_models`` and return its mean.

    Args:
        n_models: Number of synthetic models to build for this run.

    Returns:
        The mean run time in seconds, or ``None`` if the benchmark run failed.
    """
    with tempfile.TemporaryDirectory() as tmp:
        json_path = Path(tmp) / "benchmark.json"
        # No --numprocesses (xdist disables pytest-benchmark) and no --cov (skews
        # timings) — mirrors the ``test-benchmark`` mise task.
        cmd = [
            sys.executable,
            "-m",
            "pytest",
            "-c",
            "./tests",
            "--benchmark-only",
            "-k",
            _FULL_RUN_BENCHMARK,
            f"--benchmark-json={json_path}",
            "./tests/benchmark",
            "-q",
        ]
        env = {**os.environ, "DBT_BOUNCER_BENCH_MODELS": str(n_models)}
        # No capture_output/text: inherit the parent's stdio so the child's
        # tqdm progress bars (see run_bouncer_phase_decomposition in
        # conftest.py) stream live instead of being swallowed into a pipe.
        proc = subprocess.run(  # noqa: S603 - fixed argv, no shell, no untrusted input
            cmd, cwd=_REPO_ROOT, env=env
        )
        if proc.returncode != 0 or not json_path.exists():
            typer.echo(
                f"  benchmark failed for {n_models:,} models (exit {proc.returncode})",
                err=True,
            )
            return None
        data = json.loads(json_path.read_text())

    try:
        return _parse_mean(data)
    except KeyError as exc:
        typer.echo(f"  {exc}", err=True)
        return None


def main(
    models: Annotated[
        str | None,
        typer.Option(
            help="Space- or comma-separated model counts to sweep "
            "(default: 100 250 500 1000 2000 5000 10000).",
        ),
    ] = None,
) -> None:
    """Sweep the end-to-end benchmark across model counts and print a table.

    Raises:
        typer.Exit: With code 1 if any benchmark run failed.
    """
    counts = _parse_model_counts(models)
    typer.echo(
        f"Sweeping {len(counts)} model counts: {', '.join(f'{c:,}' for c in counts)}"
    )

    results: list[tuple[int, float | None]] = []
    for n in counts:
        typer.echo(f"Benchmarking {n:,} models …")
        results.append((n, _run_single(n)))

    from rich.console import Console

    Console().print(_build_table(results))

    if any(mean is None for _, mean in results):
        raise typer.Exit(code=1)


if __name__ == "__main__":
    typer.run(main)
