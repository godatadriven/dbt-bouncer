"""MCP server exposing dbt-bouncer to AI coding agents.

The payload functions in this module are plain Python and independently
testable; :func:`build_server` wraps them as MCP tools. Only
:func:`build_server` imports the optional ``mcp`` dependency.

``run_checks`` executes checks in-process with stdout/stderr redirected into a
buffer (serialized under a lock) so the stdio MCP transport stream stays clean,
and caches parsed dbt artifacts in memory keyed by file modification times and
sizes so repeated agent edit-check iterations skip disk reads and JSON decoding.
"""

from __future__ import annotations

import contextlib
import io
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dbt_bouncer.artifact_parsers.parser import ParsedArtifacts
    from dbt_bouncer.configuration_file.parser import DbtBouncerConfBase

# Bounded cache size so a long-lived MCP server switching across projects does
# not hold parsed manifests indefinitely.
_MAX_ARTIFACT_CACHE_ENTRIES = 4

# Serialises in-process runs because stdout/stderr redirection and root logger
# configuration are process-global.
_RUN_LOCK = threading.Lock()


@dataclass(frozen=True, slots=True)
class _CachedArtifacts:
    """In-memory parsed artifacts paired with their on-disk file signatures."""

    manifest_sig: tuple[int, int]
    catalog_sig: tuple[int, int] | None
    run_results_sig: tuple[int, int] | None
    info_schema_sig: tuple[tuple[str, int, int], ...] | None
    artifacts: ParsedArtifacts


_ARTIFACT_CACHE: dict[tuple[Path, str | None], _CachedArtifacts] = {}


def clear_artifact_cache() -> None:
    """Clear the in-memory MCP artifact cache."""
    with _RUN_LOCK:
        _ARTIFACT_CACHE.clear()


def _file_signature(path: Path) -> tuple[int, int] | None:
    """Return ``(mtime_ns, size)`` for *path*, or ``None`` if it cannot be statted.

    Returns:
        tuple[int, int] | None: File stat signature, or ``None``.

    """
    try:
        st = path.stat()
    except OSError:
        return None
    return (st.st_mtime_ns, st.st_size)


def _info_schema_signature(
    dbt_artifacts_dir: Path,
) -> tuple[tuple[str, int, int], ...] | None:
    """Return stat signatures for all Parquet files under ``info_schema/v1``.

    Returns:
        tuple[tuple[str, int, int], ...] | None: Sorted ``(name, mtime_ns, size)``
            entries, or ``None`` if the directory does not exist.

    """
    from dbt_bouncer.artifact_parsers.info_schema import info_schema_dir

    schema_dir = info_schema_dir(dbt_artifacts_dir)
    if not schema_dir.is_dir():
        return None
    sigs: list[tuple[str, int, int]] = []
    for p in sorted(schema_dir.glob("*.parquet")):
        sig = _file_signature(p)
        if sig is not None:
            sigs.append((p.name, sig[0], sig[1]))
    return tuple(sigs)


def _get_or_parse_artifacts(
    bouncer_config: DbtBouncerConfBase,
    dbt_artifacts_dir: Path,
) -> ParsedArtifacts:
    """Return cached parsed artifacts when on-disk files are unchanged, else re-parse.

    Args:
        bouncer_config: Validated dbt-bouncer configuration.
        dbt_artifacts_dir: Directory containing dbt artifacts.

    Returns:
        ParsedArtifacts: Parsed dbt artifacts ready for check execution.

    """
    from dbt_bouncer.artifact_parsers.parser import parse_dbt_artifacts

    resolved_dir = dbt_artifacts_dir.resolve()
    need_catalog = bool(getattr(bouncer_config, "catalog_checks", None))
    need_run_results = bool(getattr(bouncer_config, "run_results_checks", None))
    need_info_schema = bool(getattr(bouncer_config, "info_schema_checks", None))

    manifest_sig = _file_signature(resolved_dir / "manifest.json")
    catalog_sig = (
        _file_signature(resolved_dir / "catalog.json") if need_catalog else None
    )
    run_results_sig = (
        _file_signature(resolved_dir / "run_results.json") if need_run_results else None
    )
    info_schema_sig = _info_schema_signature(resolved_dir) if need_info_schema else None

    cache_key = (resolved_dir, bouncer_config.package_name)
    cached = _ARTIFACT_CACHE.get(cache_key)
    if (
        cached is not None
        and manifest_sig is not None
        and cached.manifest_sig == manifest_sig
        and (
            not need_catalog
            or (catalog_sig is not None and cached.catalog_sig == catalog_sig)
        )
        and (
            not need_run_results
            or (
                run_results_sig is not None
                and cached.run_results_sig == run_results_sig
            )
        )
        and (
            not need_info_schema
            or (
                info_schema_sig is not None
                and cached.info_schema_sig == info_schema_sig
            )
        )
    ):
        return cached.artifacts

    artifacts = parse_dbt_artifacts(
        bouncer_config=bouncer_config,
        dbt_artifacts_dir=dbt_artifacts_dir,
    )

    if manifest_sig is not None:
        if len(_ARTIFACT_CACHE) >= _MAX_ARTIFACT_CACHE_ENTRIES and (
            cache_key not in _ARTIFACT_CACHE
        ):
            oldest_key = next(iter(_ARTIFACT_CACHE))
            del _ARTIFACT_CACHE[oldest_key]
        _ARTIFACT_CACHE[cache_key] = _CachedArtifacts(
            manifest_sig=manifest_sig,
            catalog_sig=catalog_sig,
            run_results_sig=run_results_sig,
            info_schema_sig=info_schema_sig,
            artifacts=artifacts,
        )

    return artifacts


def list_checks(category: str | None = None) -> dict[str, Any]:
    """List all available dbt-bouncer checks, grouped by category.

    Args:
        category: Optional filter, one of ``catalog_checks``,
            ``info_schema_checks``, ``manifest_checks``, or ``run_results_checks``.

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
    from dbt_bouncer.utils import lookup_check_class

    check_class = lookup_check_class(check)
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
    with ``dbt parse``). Runs in-process with stdout/stderr captured so the MCP
    stdio stream stays clean, reusing cached parsed artifacts when files on
    disk have not changed.

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
    from dbt_bouncer.cli.run.utils import (
        _artifacts_dir,
        _parse_check_names,
        _parse_only,
        _prepare_bouncer_config,
        _resolve_config_file_source,
    )
    from dbt_bouncer.cli.utils import resolve_config_path
    from dbt_bouncer.context import BouncerContext
    from dbt_bouncer.enums import CheckOutcome, ExitCode, OutputFormat
    from dbt_bouncer.exceptions import DbtBouncerArtifactError, DbtBouncerConfigError
    from dbt_bouncer.reporting.logger import configure_console_logging
    from dbt_bouncer.runner import runner

    buf = io.StringIO()
    exit_code: int = ExitCode.SUCCESS
    failures: list[dict[str, Any]] = []

    with _RUN_LOCK:
        root_logger = logging.getLogger("")
        saved_handlers = list(root_logger.handlers)
        saved_level = root_logger.level
        try:
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
                configure_console_logging(verbosity=0)
                try:
                    only_parsed = _parse_only(only)
                    check_names = _parse_check_names(check)
                    resolved_config = resolve_config_path(
                        Path(config_file) if config_file else None
                    )
                    config_file_source = _resolve_config_file_source(
                        resolved_config, None
                    )
                    bouncer_config, check_categories, config_file_path = (
                        _prepare_bouncer_config(
                            resolved_config,
                            config_file_source,
                            only_parsed,
                            check_names,
                            preset=None,
                        )
                    )
                    artifacts = _get_or_parse_artifacts(
                        bouncer_config=bouncer_config,
                        dbt_artifacts_dir=_artifacts_dir(
                            bouncer_config, config_file_path
                        ),
                    )
                    ctx = BouncerContext.model_construct(
                        allow_zero_checks=False,
                        bouncer_config=bouncer_config,
                        catalog_nodes=artifacts.catalog_nodes,
                        catalog_sources=artifacts.catalog_sources,
                        check_categories=check_categories,
                        create_pr_comment_file=False,
                        dry_run=False,
                        exposures=artifacts.exposures,
                        info_schema=artifacts.info_schema,
                        macros=artifacts.macros,
                        manifest_obj=artifacts.manifest_obj,
                        models=artifacts.models,
                        output_file=None,
                        output_format=OutputFormat.JSON.value,
                        output_only_failures=True,
                        run_results=artifacts.run_results,
                        seeds=artifacts.seeds,
                        semantic_models=artifacts.semantic_models,
                        show_all_failures=False,
                        snapshots=artifacts.snapshots,
                        sources=artifacts.sources,
                        tests=artifacts.tests,
                        unit_tests=artifacts.unit_tests,
                    )
                    exit_code, results = runner(ctx=ctx)
                    failures = [
                        r for r in results if r["outcome"] != CheckOutcome.SUCCESS
                    ]
                except DbtBouncerConfigError as e:
                    logging.error(str(e))
                    exit_code = int(ExitCode.CONFIG_ERROR)
                except DbtBouncerArtifactError as e:
                    logging.error(str(e))
                    exit_code = int(ExitCode.ARTIFACT_ERROR)
        finally:
            root_logger.handlers = saved_handlers
            root_logger.setLevel(saved_level)

    console_output = buf.getvalue().strip()
    return {
        "console_output_tail": "\n".join(console_output.splitlines()[-15:]),
        "exit_code": int(exit_code),
        "failures": failures,
        "passed": exit_code == 0,
    }


def build_server() -> Any:
    """Build the MCP server with all dbt-bouncer tools registered.

    The server class differs by mcp version. mcp<2 provides ``FastMCP``. mcp>=2
    renames it to ``MCPServer``. Both expose the same ``.tool()``, ``.run()``,
    and ``.list_tools()`` methods that this function and its callers use.

    Returns:
        Any: The configured server. Call ``.run()`` to serve on stdio.

    """
    try:
        # mcp<2 provides FastMCP; mcp>=2 renamed it to MCPServer (below).
        from mcp.server.fastmcp import FastMCP as McpServer  # ty: ignore
    except ModuleNotFoundError:
        # A missing top-level `mcp` package re-raises here and is handled by the
        # `mcp` command as a CONFIG_ERROR.
        from mcp.server import MCPServer as McpServer

    # Resolve a forward reference in FastMCP's own `Settings` model (mcp<2 only).
    # Without this, pydantic-settings emits an IncompleteFieldDefinitionWarning
    # for the `lifespan` field when the server is built (upstream mcp issue). The
    # rebuild is a no-op if the reference already resolves. Guarded so a future
    # mcp refactor that renames or removes the model cannot break server startup.
    # mcp>=2 has no such module, so the import fails and is skipped.
    try:
        from mcp.server.fastmcp.server import Settings  # ty: ignore[unresolved-import]

        Settings.model_rebuild()
    except Exception:  # pragma: no cover - defensive against mcp internals
        # The warning is cosmetic, so a failed rebuild must not stop the server.
        logging.debug("FastMCP Settings.model_rebuild() failed.", exc_info=True)

    server = McpServer(
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
