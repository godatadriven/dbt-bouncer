"""MCP server exposing dbt-bouncer to AI coding agents.

The payload functions in this module are plain Python and independently
testable; :func:`build_server` wraps them as MCP tools. Only
:func:`build_server` imports the optional ``mcp`` dependency.

The ``run_checks`` tool shells out to the ``dbt-bouncer`` executable instead
of calling ``run_bouncer()`` in-process: the stdio MCP transport owns this
process's stdout, and an in-process run would corrupt the protocol stream
with log lines and progress output.
"""

from __future__ import annotations

import json
import shutil

# The subprocess module is used deliberately: an in-process run would corrupt
# the MCP stdio protocol stream (see module docstring).
import subprocess  # ruff: ignore[suspicious-subprocess-import] # nosec B404
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mcp.server.fastmcp import FastMCP

# Two minutes: comfortably above a full run on a very large dbt project,
# small enough that a hung subprocess does not stall the agent forever.
_RUN_TIMEOUT_SECONDS = 120


def list_checks(category: str | None = None) -> dict[str, Any]:
    """List all available dbt-bouncer checks, grouped by category.

    Args:
        category: Optional filter, one of ``catalog_checks``,
            ``manifest_checks``, or ``run_results_checks``.

    Returns:
        dict[str, Any]: Category label to list of checks (code, name,
        description, parameters).

    """
    from dbt_bouncer.cli.list.utils import build_checks_payload, category_key
    from dbt_bouncer.enums import CheckCategory
    from dbt_bouncer.utils import get_check_objects

    category_labels = {c.directory: c.value for c in CheckCategory}
    selected = frozenset({category}) if category else None

    checks = sorted(
        get_check_objects(check_categories=selected),
        key=lambda c: (category_key(c), c.__name__),
    )
    payload: dict[str, Any] = dict(build_checks_payload(checks, category_labels))
    if category:
        payload = {k: v for k, v in payload.items() if k == category}
    return payload


def explain_check(check: str) -> dict[str, Any]:
    """Explain one dbt-bouncer check: description, parameters, example usage.

    Args:
        check: Check name (e.g. ``check_model_names``) or rule code
            (e.g. ``MO038``).

    Returns:
        dict[str, Any]: The check's name, code, category, full docstring
        (including YAML examples), configurable parameters, and documentation
        URL — or an ``error`` key when the check is unknown.

    """
    # Delegate to the `explain` subcommand's payload builder (#1070) so both
    # surfaces describe a check identically.
    from dbt_bouncer.cli.explain.utils import build_explain_payload
    from dbt_bouncer.utils import get_check_registry

    registry = get_check_registry()
    check_class = registry.get(check)
    if check_class is None:
        return {
            "error": f"Unknown check name or rule code '{check}'. Use the list_checks tool to see all checks."
        }

    return dict(build_explain_payload(check_class))


def read_project_config(config_file: str | None = None) -> dict[str, Any]:
    """Read and validate the project's dbt-bouncer config file.

    This shows which conventions the project enforces, so generated dbt code
    can comply with them up front.

    Args:
        config_file: Path to the config file. When omitted, the standard
            resolution order applies (``dbt-bouncer.yml`` in the current
            working directory, then ``.yaml``/``.toml``/``pyproject.toml``).

    Returns:
        dict[str, Any]: The resolved path, the raw config contents, whether
        the config validates, and any validation errors.

    """
    from dbt_bouncer.configuration_file.validator import (
        get_config_file_path,
        load_config_file_contents,
        validate_conf,
    )
    from dbt_bouncer.enums import ConfigFileName, ConfigFileSource
    from dbt_bouncer.exceptions import DbtBouncerConfigError

    try:
        config_file_path = Path(
            get_config_file_path(
                config_file=Path(config_file)
                if config_file
                else Path(ConfigFileName.DBT_BOUNCER_YML),
                config_file_source=ConfigFileSource.COMMANDLINE
                if config_file
                else ConfigFileSource.DEFAULT,
            )
        )
        config_file_contents = dict(
            load_config_file_contents(
                config_file_path, allow_default_config_file_creation=False
            )
        )
    except DbtBouncerConfigError as e:
        return {"config_file": config_file, "error": str(e), "valid": False}

    check_categories = [
        k
        for k in config_file_contents
        if k.endswith("_checks") and config_file_contents.get(k) != []
    ]

    custom_checks_dir = None
    if config_file_contents.get("custom_checks_dir"):
        custom_checks_dir = (
            config_file_path.parent / config_file_contents["custom_checks_dir"]
        )

    errors: list[str] = []
    try:
        validate_conf(
            check_categories=check_categories,
            config_file_contents=dict(config_file_contents),
            custom_checks_dir=custom_checks_dir,
        )
    except DbtBouncerConfigError as e:
        errors = str(e).splitlines()

    return {
        "config": config_file_contents,
        "config_file": str(config_file_path),
        "errors": errors,
        "valid": not errors,
    }


def run_checks(
    config_file: str | None = None,
    only: str = "",
    check: str = "",
) -> dict[str, Any]:
    """Run dbt-bouncer checks against the project's dbt artifacts.

    Requires dbt artifacts to exist (at minimum ``manifest.json``, generated
    with ``dbt parse``). Runs in a subprocess so the MCP stdio stream stays
    clean.

    Args:
        config_file: Path to the config file. When omitted, the standard
            resolution order applies.
        only: Comma-separated check categories to run (e.g.
            ``manifest_checks``).
        check: Comma-separated check names to run.

    Returns:
        dict[str, Any]: The exit code, whether all checks passed, the list of
        failed checks, and the tail of the console output.

    """
    executable = shutil.which("dbt-bouncer")
    if executable is None:
        return {
            "error": "The `dbt-bouncer` executable was not found on PATH. Install it with `pip install dbt-bouncer`."
        }

    with tempfile.TemporaryDirectory() as tmp_dir:
        output_file = Path(tmp_dir) / "results.json"
        cmd = [
            executable,
            "run",
            "--output-file",
            str(output_file),
            "--output-only-failures",
        ]
        if config_file:
            cmd.extend(["--config-file", config_file])
        if only:
            cmd.extend(["--only", only])
        if check:
            cmd.extend(["--check", check])

        try:
            # No shell, fixed argument list, executable resolved via
            # shutil.which — not user-controlled input.
            completed = subprocess.run(  # ruff: ignore[subprocess-without-shell-equals-true] # nosec B603
                cmd,
                capture_output=True,
                text=True,
                timeout=_RUN_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return {
                "error": f"dbt-bouncer did not finish within {_RUN_TIMEOUT_SECONDS} seconds."
            }

        failures: list[dict[str, Any]] = []
        if output_file.exists():
            failures = json.loads(output_file.read_text())

    console_output = (completed.stdout + completed.stderr).strip()
    return {
        "console_output_tail": "\n".join(console_output.splitlines()[-15:]),
        "exit_code": completed.returncode,
        "failures": failures,
        "passed": completed.returncode == 0,
    }


def build_server() -> FastMCP:
    """Build the FastMCP server with all dbt-bouncer tools registered.

    Returns:
        FastMCP: The configured server. Call ``.run()`` to serve on stdio.

    """
    from mcp.server.fastmcp import FastMCP

    server = FastMCP(
        "dbt-bouncer",
        instructions=(
            "dbt-bouncer enforces conventions in dbt projects. Use "
            "read_project_config to learn which conventions this project "
            "enforces before generating dbt code, list_checks/explain_check "
            "to understand available checks, and run_checks to verify the "
            "project against its configured conventions (requires dbt "
            "artifacts, at minimum a manifest.json from `dbt parse`)."
        ),
    )
    server.tool()(explain_check)
    server.tool()(list_checks)
    server.tool()(read_project_config)
    server.tool()(run_checks)
    return server
