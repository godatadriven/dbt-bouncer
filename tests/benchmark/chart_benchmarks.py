"""Generate a plot of benchmark runtime per iteration (distribution/histogram).

Runs the benchmark (or accepts pre-existing pytest-benchmark JSON output), constructs
an Altair performance density scatter plot (saved to HTML), and displays the
scatter plot and statistical summary directly in the terminal.

Run via mise (recommended)::

    mise run test-benchmark-chart
    mise run test-benchmark-chart --models 500

or directly::

    uv run python tests/benchmark/chart_benchmarks.py --models 1000 --benchmark test_run_bouncer
"""

from __future__ import annotations

import json
import math
import os
import subprocess  # ruff: ignore[suspicious-subprocess-import] - fixed, fully-controlled command (no shell, no user input)
import sys
import tempfile
from pathlib import Path
from typing import Annotated, Any

import altair as alt
import typer

_DEFAULT_BENCHMARK = "test_run_bouncer"
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _format_time(seconds: float) -> str:
    """Format a time duration in seconds, ms, or μs.

    Args:
        seconds: Duration in seconds.

    Returns:
        Formatted string (e.g., "1.23 ms", "456.78 μs", "2.50 s").

    """
    if seconds >= 1.0:
        return f"{seconds:.3f} s"
    elif seconds >= 0.001:
        return f"{seconds * 1000:.2f} ms"
    else:
        return f"{seconds * 1_000_000:.2f} μs"


def _compute_stats(data: list[float]) -> dict[str, float]:
    """Compute summary statistics and outlier boundaries for per-iteration timings.

    Args:
        data: List of per-iteration timings in seconds.

    Returns:
        Dictionary containing statistical metrics and outlier bounds.

    """
    if not data:
        return {}

    sorted_data = sorted(data)
    n = len(sorted_data)
    min_val = sorted_data[0]
    max_val = sorted_data[-1]
    mean_val = sum(sorted_data) / n

    if n % 2 == 1:
        median_val = sorted_data[n // 2]
    else:
        median_val = (sorted_data[n // 2 - 1] + sorted_data[n // 2]) / 2.0

    variance = sum((x - mean_val) ** 2 for x in sorted_data) / (n - 1) if n > 1 else 0.0
    stddev = math.sqrt(variance)

    q1 = sorted_data[n // 4]
    q3 = sorted_data[(3 * n) // 4]
    iqr = q3 - q1

    mild_lower = q1 - 1.5 * iqr
    mild_upper = q3 + 1.5 * iqr
    severe_lower = q1 - 3.0 * iqr
    severe_upper = q3 + 3.0 * iqr

    return {
        "count": float(n),
        "iqr": iqr,
        "max": max_val,
        "mean": mean_val,
        "median": median_val,
        "mild_lower": mild_lower,
        "mild_upper": mild_upper,
        "min": min_val,
        "q1": q1,
        "q3": q3,
        "severe_lower": severe_lower,
        "severe_upper": severe_upper,
        "stddev": stddev,
    }


def _create_altair_chart(
    benchmark_name: str, timings: list[float], stats: dict[str, float]
) -> alt.Chart:
    """Construct an Altair density scatter plot chart with outlier boundaries.

    Args:
        benchmark_name: Name of the benchmark being plotted.
        timings: Per-iteration timing samples in seconds.
        stats: Dictionary of statistical metrics.

    Returns:
        Layered Altair Chart object with independent Y-axes.

    """
    n = len(timings)
    mean_val = stats["mean"]
    stddev = stats["stddev"] or 1e-6
    h = 1.06 * stddev * (n ** (-0.2)) if n > 0 else 1e-6

    sorted_t = sorted(timings)
    min_t, max_t = sorted_t[0], sorted_t[-1]
    span = max_t - min_t or 1e-6

    min_grid = min_t - span * 0.1
    max_grid = max_t + span * 0.1
    num_grid = 200

    density_data: list[dict[str, float]] = []
    for i in range(num_grid):
        t = min_grid + (i / (num_grid - 1)) * (max_grid - min_grid)
        d = (
            sum(
                math.exp(-0.5 * ((t - x) / h) ** 2) / (math.sqrt(2 * math.pi) * h)
                for x in sorted_t
            )
            / n
        )
        density_data.append({"Execution Time": t, "Density": d})

    points_data: list[dict[str, Any]] = []
    for idx, x in enumerate(timings):
        if x < stats["severe_lower"] or x > stats["severe_upper"]:
            cat = "Severe outliers"
        elif x < stats["mild_lower"] or x > stats["mild_upper"]:
            cat = "Mild outliers"
        else:
            cat = "Clean sample"

        points_data.append(
            {
                "Category": cat,
                "Execution Time": x,
                "Iteration": idx + 1,
            }
        )

    # Left Y-axis: Probability Density
    density_chart = (
        alt.Chart(alt.Data(values=density_data))
        .mark_area(opacity=0.35, color="#6baed6")
        .encode(
            x=alt.X("Execution Time:Q", title="Execution Time (seconds)"),
            y=alt.Y("Density:Q", title="Probability Density (a.u.)"),
        )
    )

    # Right Y-axis: Iteration
    points_chart = (
        alt.Chart(alt.Data(values=points_data))
        .mark_point(filled=True, size=55)
        .encode(
            x="Execution Time:Q",
            y=alt.Y("Iteration:Q", title="Iteration"),
            color=alt.Color(
                "Category:N",
                scale=alt.Scale(
                    domain=["Clean sample", "Mild outliers", "Severe outliers"],
                    range=["#1f77b4", "#ff7f0e", "#d62728"],
                ),
                title="Legend",
            ),
            tooltip=["Iteration:Q", "Execution Time:Q", "Category:N"],
        )
    )

    # Mean rule line
    mean_chart = (
        alt.Chart(alt.Data(values=[{"time": mean_val}]))
        .mark_rule(color="#1f77b4", strokeWidth=2)
        .encode(x="time:Q")
    )

    # Outlier threshold rule lines
    rules_data: list[dict[str, Any]] = []
    if stats["mild_lower"] > min_grid:
        rules_data.append({"color": "#ff7f0e", "time": stats["mild_lower"]})
    if stats["mild_upper"] < max_grid:
        rules_data.append({"color": "#ff7f0e", "time": stats["mild_upper"]})
    if stats["severe_lower"] > min_grid:
        rules_data.append({"color": "#d62728", "time": stats["severe_lower"]})
    if stats["severe_upper"] < max_grid:
        rules_data.append({"color": "#d62728", "time": stats["severe_upper"]})

    rules_chart = (
        alt.Chart(alt.Data(values=rules_data))
        .mark_rule(strokeDash=[4, 4], strokeWidth=1.5)
        .encode(
            x="time:Q",
            color=alt.Color("color:N", scale=None),
        )
    )

    return (
        alt.layer(density_chart, points_chart, mean_chart, rules_chart)
        .resolve_scale(y="independent")
        .properties(
            title=f"dbt-bouncer Benchmark Performance Scatter Plot — {benchmark_name}",
            width=720,
            height=420,
        )
        .configure_axis(grid=True, gridOpacity=0.2)
    )


def _render_terminal_scatter_chart(
    benchmark_name: str,
    timings: list[float],
    stats: dict[str, float],
    cols: int = 64,
    rows: int = 12,
) -> None:
    """Render a visual performance scatter plot and density curve directly in the terminal.

    Args:
        benchmark_name: Name of the benchmark being displayed.
        timings: Execution timing samples in seconds.
        stats: Dictionary of statistical metrics.
        cols: Width of terminal plot grid in characters.
        rows: Height of terminal plot grid in characters.

    """
    from rich.console import Console
    from rich.panel import Panel
    from rich.table import Table

    n = int(stats["count"])
    sorted_t = sorted(timings)
    min_t, max_t = sorted_t[0], sorted_t[-1]
    span = max_t - min_t or 1e-6
    stddev = stats["stddev"] or 1e-6
    h = 1.06 * stddev * (n ** (-0.2)) if n > 0 else 1e-6

    densities = []
    for c in range(cols):
        t = min_t + (c / max(1, cols - 1)) * span
        d = (
            sum(
                math.exp(-0.5 * ((t - x) / h) ** 2) / (math.sqrt(2 * math.pi) * h)
                for x in sorted_t
            )
            / n
        )
        densities.append(d)

    max_d = max(densities) or 1.0

    def val_to_col(v: float) -> int:
        return max(0, min(cols - 1, round(((v - min_t) / span) * (cols - 1))))

    grid = [[" " for _ in range(cols)] for _ in range(rows)]
    colors = [["" for _ in range(cols)] for _ in range(rows)]

    # 1. Fill density PDF background
    for c in range(cols):
        h_val = round((densities[c] / max_d) * (rows - 1))
        for r in range(h_val):
            r_idx = rows - 1 - r
            grid[r_idx][c] = "░"
            colors[r_idx][c] = "blue"
        if h_val > 0:
            r_idx = rows - 1 - h_val
            grid[r_idx][c] = "▄"
            colors[r_idx][c] = "cyan"

    # 2. Draw vertical mean & outlier lines
    mean_c = val_to_col(stats["mean"])
    mild_l_c = val_to_col(stats["mild_lower"])
    mild_u_c = val_to_col(stats["mild_upper"])
    sev_l_c = val_to_col(stats["severe_lower"])
    sev_u_c = val_to_col(stats["severe_upper"])

    for r in range(rows):
        if grid[r][mean_c] in (" ", "░", "▄"):
            grid[r][mean_c] = "│"
            colors[r][mean_c] = "bold cyan"
        if 0 <= mild_l_c < cols and grid[r][mild_l_c] in (" ", "░", "▄"):
            grid[r][mild_l_c] = "┆"
            colors[r][mild_l_c] = "yellow"
        if 0 <= mild_u_c < cols and grid[r][mild_u_c] in (" ", "░", "▄"):
            grid[r][mild_u_c] = "┆"
            colors[r][mild_u_c] = "yellow"
        if 0 <= sev_l_c < cols and grid[r][sev_l_c] in (" ", "░", "▄"):
            grid[r][sev_l_c] = "┆"
            colors[r][sev_l_c] = "red"
        if 0 <= sev_u_c < cols and grid[r][sev_u_c] in (" ", "░", "▄"):
            grid[r][sev_u_c] = "┆"
            colors[r][sev_u_c] = "red"

    # 3. Plot scatter points
    for idx, x in enumerate(timings):
        c = val_to_col(x)
        if x < stats["severe_lower"] or x > stats["severe_upper"]:
            char, col = "▲", "bold red"
        elif x < stats["mild_lower"] or x > stats["mild_upper"]:
            char, col = "●", "bold yellow"
        else:
            char, col = "•", "bold white"

        r_idx = rows - 1 - int((idx / max(1, n - 1)) * (rows - 1))
        grid[r_idx][c] = char
        colors[r_idx][c] = col

    lines = []
    for r in range(rows):
        line_str = ""
        for c in range(cols):
            ch = grid[r][c]
            co = colors[r][c]
            if co:
                line_str += f"[{co}]{ch}[/{co}]"
            else:
                line_str += ch
        lines.append(line_str)

    x_axis_str = (
        f"[dim]{_format_time(min_t)}[/dim]"
        + " " * max(1, (cols // 2) - 12)
        + f"[bold cyan]Mean: {_format_time(stats['mean'])}[/bold cyan]"
        + " " * max(1, (cols // 2) - 16)
        + f"[dim]{_format_time(max_t)}[/dim]"
    )

    plot_content = "\n".join(lines) + "\n" + x_axis_str

    console = Console()
    console.print(
        Panel(
            plot_content,
            title=f"[bold cyan]Altair Performance Scatter Plot — {benchmark_name}[/bold cyan]",
            subtitle="[dim]░ PDF Area  │ Mean  • Clean Sample  ● Mild Outlier  ▲ Severe Outlier[/dim]",
            subtitle_align="left",
            border_style="cyan",
        )
    )

    table = Table(show_header=False, box=None)
    table.add_column(style="cyan")
    table.add_column(style="bold white")
    table.add_column(style="cyan")
    table.add_column(style="bold white")

    table.add_row(
        "Rounds:",
        f"{n}",
        "Mean:",
        _format_time(stats["mean"]),
    )
    table.add_row(
        "Median:",
        _format_time(stats["median"]),
        "StdDev:",
        _format_time(stats["stddev"]),
    )
    table.add_row(
        "Min:",
        _format_time(stats["min"]),
        "Max:",
        _format_time(stats["max"]),
    )
    console.print(table)


def _extract_benchmarks(data: dict) -> dict[str, dict[str, Any]]:
    """Extract per-iteration timings and stats from pytest-benchmark JSON report.

    Args:
        data: Parsed pytest-benchmark JSON output.

    Returns:
        Dict mapping benchmark name to dict with 'data' (per-iteration timing list in seconds) and stats.

    """
    results: dict[str, dict[str, Any]] = {}
    for bench in data.get("benchmarks", []):
        name = bench.get("name", "")
        stats = bench.get("stats", {})
        raw_data = stats.get("data", [])
        iterations = max(1, int(stats.get("iterations", 1)))
        if name and raw_data:
            per_iter_data = [float(x) / iterations for x in raw_data]
            results[name] = {
                "data": per_iter_data,
                "stats": stats,
            }
    return results


def _run_benchmark_json(
    n_models: int,
    phase_rounds: int,
    benchmark_name: str,
    benchmark_rounds: int | None = None,
) -> dict | None:
    """Run pytest-benchmark and return the raw JSON report dict.

    Args:
        n_models: Number of synthetic models to generate.
        phase_rounds: Number of phase rounds for benchmarking.
        benchmark_name: Specific benchmark name or 'all'.
        benchmark_rounds: Minimum number of benchmark rounds to run.

    Returns:
        Parsed JSON dictionary, or None on failure.

    """
    with tempfile.TemporaryDirectory() as tmp:
        json_path = Path(tmp) / "benchmark.json"
        cmd = [
            sys.executable,
            "-m",
            "pytest",
            "-c",
            "./tests",
            "--benchmark-only",
            f"--benchmark-json={json_path}",
            "./tests/benchmark",
            "-q",
        ]
        if benchmark_rounds is not None:
            cmd.append(f"--benchmark-min-rounds={benchmark_rounds}")
        if benchmark_name != "all":
            cmd.extend(["-k", benchmark_name])

        env = {
            **os.environ,
            "DBT_BOUNCER_BENCH_MODELS": str(n_models),
            "DBT_BOUNCER_BENCH_PHASE_ROUNDS": str(phase_rounds),
        }

        proc = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true] - fixed argv, no shell, no untrusted input
            cmd,
            cwd=_REPO_ROOT,
            env=env,
            stdout=subprocess.DEVNULL,
        )
        if proc.returncode != 0 or not json_path.exists():
            typer.echo(
                f"  benchmark failed (exit code {proc.returncode})",
                err=True,
            )
            return None
        return json.loads(json_path.read_text())


app = typer.Typer()


@app.command()
def main(
    models: Annotated[
        int,
        typer.Option(
            help="Number of synthetic models to generate for the benchmark.",
        ),
    ] = 1000,
    phase_rounds: Annotated[
        int,
        typer.Option(
            help="Number of phase rounds to run.",
        ),
    ] = 50,
    benchmark_name: Annotated[
        str,
        typer.Option(
            "--benchmark",
            help="Benchmark test name to plot (e.g. 'test_run_bouncer' or 'all').",
        ),
    ] = _DEFAULT_BENCHMARK,
    output_html: Annotated[
        Path | None,
        typer.Option(
            "--output",
            "-o",
            help="Path to save the generated Altair chart HTML file (default: 'benchmark_chart.html').",
        ),
    ] = Path("benchmark_chart.html"),
    benchmark_rounds: Annotated[
        int | None,
        typer.Option(
            "--benchmark-rounds",
            "--rounds",
            help="Minimum number of benchmark rounds (samples) to run for pytest-benchmark.",
        ),
    ] = None,
    json_file: Annotated[
        Path | None,
        typer.Option(
            help="Path to pre-existing pytest-benchmark JSON report to plot directly.",
        ),
    ] = None,
) -> None:
    """Generate a performance scatter plot using Altair and display it in the terminal.

    Raises:
        typer.Exit: With code 1 if benchmark fails or no timing data is found.

    """
    if json_file is not None:
        if not json_file.exists():
            typer.echo(f"Error: JSON file {json_file} does not exist.", err=True)
            raise typer.Exit(code=1)
        raw_json = json.loads(json_file.read_text())
    else:
        rounds_msg = (
            f" ({benchmark_rounds} benchmark rounds)"
            if benchmark_rounds is not None
            else ""
        )
        typer.echo(
            f"Running benchmark '{benchmark_name}' with {models:,} models ({phase_rounds} phase rounds){rounds_msg} …"
        )
        raw_json = _run_benchmark_json(
            models, phase_rounds, benchmark_name, benchmark_rounds
        )
        if raw_json is None:
            raise typer.Exit(code=1)

    benchmarks_data = _extract_benchmarks(raw_json)

    if not benchmarks_data:
        typer.echo("Error: No benchmark data found in results.", err=True)
        raise typer.Exit(code=1)

    from rich.console import Console

    console = Console()

    selected_b = (
        [benchmark_name]
        if benchmark_name != "all" and benchmark_name in benchmarks_data
        else list(benchmarks_data.keys())
    )

    for b_name in selected_b:
        entry = benchmarks_data[b_name]
        timings = entry["data"]
        stats = _compute_stats(timings)

        # Build Altair chart
        chart = _create_altair_chart(b_name, timings, stats)

        # Save HTML chart
        target_html = (
            output_html
            if output_html is not None and len(selected_b) == 1
            else Path(f"benchmark_chart_{b_name}.html")
        )

        if target_html is not None:
            chart.save(str(target_html))

        # Render rich terminal scatter chart
        _render_terminal_scatter_chart(b_name, timings, stats)

        if target_html is not None:
            console.print(
                f"[bold green]✓ Altair chart saved to:[/bold green] [bold cyan]{target_html.resolve()}[/bold cyan]"
            )
            console.print()


if __name__ == "__main__":
    app()
