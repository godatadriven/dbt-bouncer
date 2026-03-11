"""Fast artifact parser using orjson + lightweight proxy classes.

Bypasses heavy Pydantic validation by wrapping raw JSON dicts in proxy
objects that support attribute access, .value (Pydantic enum compat),
and dict/list operations.
"""

from __future__ import annotations

import io
import logging
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, NamedTuple

import orjson

from dbt_bouncer.utils import clean_path_str, get_package_version_number

if TYPE_CHECKING:
    from pathlib import Path

    from dbt_bouncer.config_file_parser import DbtBouncerConfBase


class ProxyStr(str):
    """String subclass with .value for Pydantic enum compatibility."""

    @property
    def value(self) -> str:
        """Return the string value (mimics Pydantic enum .value access).

        Returns:
            str: The underlying string value.

        """
        return str(self)


class DictProxy(dict):
    """Dict subclass with attribute access and lazy proxy wrapping."""

    def __getattr__(self, name: str) -> Any:
        """Look up *name* as a dict key, falling back to underscore-stripped alias.

        Missing keys return ``None`` to match the Pydantic default behaviour
        where optional fields default to ``None``.

        Returns:
            Any: Proxy-wrapped value, or ``None`` if the key is absent.

        """
        try:
            return _wrap_value(dict.__getitem__(self, name))
        except KeyError:
            # Trailing underscore alias (e.g. schema_ -> schema)
            stripped = name.rstrip("_")
            if stripped != name:
                try:
                    return _wrap_value(dict.__getitem__(self, stripped))
                except KeyError:
                    pass
        # Missing keys return None (matches Pydantic optional field defaults)
        return None

    def __getitem__(self, key: Any) -> Any:
        """Return a proxy-wrapped value for *key*.

        Returns:
            Any: Proxy-wrapped value.

        """
        return _wrap_value(dict.__getitem__(self, key))

    def items(self) -> Any:
        """Yield (key, wrapped_value) pairs.

        Yields:
            tuple[str, Any]: Key and proxy-wrapped value.

        """
        for k, v in dict.items(self):
            yield k, _wrap_value(v)

    def values(self) -> Any:
        """Yield proxy-wrapped values.

        Yields:
            Any: Proxy-wrapped value.

        """
        for v in dict.values(self):
            yield _wrap_value(v)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a wrapped value by key, or *default* if missing.

        Returns:
            Any: Proxy-wrapped value or *default*.

        """
        try:
            return _wrap_value(dict.__getitem__(self, key))
        except KeyError:
            return default


class ListProxy(list):
    """List subclass with lazy proxy wrapping of elements."""

    def __getitem__(self, key: Any) -> Any:  # type: ignore[invalid-method-override]
        """Return proxy-wrapped element(s) at *key*.

        Returns:
            Any: Proxy-wrapped element or list of elements.

        """
        val = list.__getitem__(self, key)
        if isinstance(key, slice):
            return [_wrap_value(v) for v in val]
        return _wrap_value(val)

    def __iter__(self) -> Any:
        """Yield proxy-wrapped elements.

        Yields:
            Any: Proxy-wrapped element.

        """
        for item in list.__iter__(self):
            yield _wrap_value(item)


def _wrap_value(value: Any) -> Any:
    """Wrap dicts/lists in proxies, strings in ProxyStr. Primitives pass through.

    Returns:
        Any: Proxy-wrapped value or primitive as-is.

    """
    if isinstance(value, dict) and not isinstance(value, DictProxy):
        return DictProxy(value)
    if isinstance(value, list) and not isinstance(value, ListProxy):
        return ListProxy(value)
    if isinstance(value, str) and not isinstance(value, ProxyStr):
        return ProxyStr(value)
    return value


def _make_wrapper(
    unique_id: str, original_file_path: str, **kwargs: Any
) -> SimpleNamespace:
    """Create a lightweight resource wrapper compatible with DbtBouncer* wrappers.

    Returns:
        SimpleNamespace: Wrapper with unique_id, original_file_path, and resource attrs.

    """
    return SimpleNamespace(
        original_file_path=original_file_path,
        unique_id=unique_id,
        **kwargs,
    )


def wrap_dict(data: dict[str, Any]) -> DictProxy:
    """Wrap a plain dict as a DictProxy with attribute access.

    Returns:
        DictProxy: The wrapped dict with attribute access support.

    """
    return DictProxy(data)


class ParsedArtifacts(NamedTuple):
    """Parsed dbt artifacts returned by :func:`parse_dbt_artifacts`."""

    manifest_obj: SimpleNamespace
    exposures: list[DictProxy]
    macros: list[DictProxy]
    models: list[SimpleNamespace]
    seeds: list[SimpleNamespace]
    semantic_models: list[SimpleNamespace]
    snapshots: list[SimpleNamespace]
    sources: list[SimpleNamespace]
    tests: list[SimpleNamespace]
    unit_tests: list[DictProxy]
    catalog_nodes: list[SimpleNamespace]
    catalog_sources: list[SimpleNamespace]
    run_results: list[SimpleNamespace]


def parse_dbt_artifacts(
    bouncer_config: DbtBouncerConfBase,
    dbt_artifacts_dir: Path,
) -> ParsedArtifacts:
    """Parse all dbt artifacts using orjson + proxy, bypassing Pydantic validation.

    Returns:
        ParsedArtifacts: Named tuple of lightweight proxy objects.

    Raises:
        AssertionError: If the dbt version is below the minimum supported version.
        FileNotFoundError: If a required artifact file does not exist.

    """
    # --- Manifest ---
    manifest_path = dbt_artifacts_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"No manifest.json found at {manifest_path}.")

    manifest_dict = orjson.loads(manifest_path.read_bytes())

    dbt_version = manifest_dict["metadata"]["dbt_version"]
    if not get_package_version_number(dbt_version) >= get_package_version_number(
        "1.7.0"
    ):
        raise AssertionError(
            f"The supplied `manifest.json` was generated with dbt version {dbt_version}, "
            "this is below the minimum supported version of 1.7.0."
        )

    manifest_proxy = DictProxy(manifest_dict)
    manifest_obj = SimpleNamespace(manifest=manifest_proxy)

    target_package = (
        bouncer_config.package_name or manifest_dict["metadata"]["project_name"]
    )

    # Extract resources from manifest
    project_exposures: list[DictProxy] = [
        DictProxy(v)
        for _, v in manifest_dict.get("exposures", {}).items()
        if v.get("package_name") == target_package
    ]

    project_macros: list[DictProxy] = [
        DictProxy(v)
        for _, v in manifest_dict.get("macros", {}).items()
        if v.get("package_name") == target_package
    ]

    project_models: list[SimpleNamespace] = []
    project_seeds: list[SimpleNamespace] = []
    project_snapshots: list[SimpleNamespace] = []
    project_tests: list[SimpleNamespace] = []

    for k, v in manifest_dict.get("nodes", {}).items():
        if v.get("package_name") != target_package:
            continue
        ofp = clean_path_str(v.get("original_file_path", ""))
        match v.get("resource_type"):
            case "model":
                project_models.append(
                    _make_wrapper(
                        unique_id=k, original_file_path=ofp, model=DictProxy(v)
                    )
                )
            case "seed":
                project_seeds.append(
                    _make_wrapper(
                        unique_id=k, original_file_path=ofp, seed=DictProxy(v)
                    )
                )
            case "snapshot":
                project_snapshots.append(
                    _make_wrapper(
                        unique_id=k, original_file_path=ofp, snapshot=DictProxy(v)
                    )
                )
            case "test":
                project_tests.append(
                    _make_wrapper(
                        unique_id=k, original_file_path=ofp, test=DictProxy(v)
                    )
                )

    project_semantic_models: list[SimpleNamespace] = [
        _make_wrapper(
            unique_id=k,
            original_file_path=clean_path_str(v.get("original_file_path", "")),
            semantic_model=DictProxy(v),
        )
        for k, v in manifest_dict.get("semantic_models", {}).items()
        if v.get("package_name") == target_package
    ]

    project_sources: list[SimpleNamespace] = [
        _make_wrapper(
            unique_id=k,
            original_file_path=clean_path_str(v.get("original_file_path", "")),
            source=DictProxy(v),
        )
        for k, v in manifest_dict.get("sources", {}).items()
        if v.get("package_name") == target_package
    ]

    # Unit tests (dbt >= 1.8.0)
    if get_package_version_number(dbt_version) >= get_package_version_number("1.8.0"):
        project_unit_tests: list[DictProxy] = [
            DictProxy(v)
            for _, v in manifest_dict.get("unit_tests", {}).items()
            if v.get("package_name") == target_package
        ]
    else:
        project_unit_tests = []

    # --- Catalog ---
    if (
        hasattr(bouncer_config, "catalog_checks")
        and bouncer_config.catalog_checks != []
    ):
        catalog_path = dbt_artifacts_dir / "catalog.json"
        if not catalog_path.exists():
            raise FileNotFoundError(f"No catalog.json found at {catalog_path}.")

        catalog_dict = orjson.loads(catalog_path.read_bytes())
        nodes_dict = manifest_dict.get("nodes", {})
        sources_dict = manifest_dict.get("sources", {})

        project_catalog_nodes: list[SimpleNamespace] = [
            _make_wrapper(
                unique_id=k,
                original_file_path=clean_path_str(nodes_dict[k]["original_file_path"])
                if k in nodes_dict
                else "",
                catalog_node=DictProxy(v),
            )
            for k, v in catalog_dict.get("nodes", {}).items()
            if k.split(".")[-2] == target_package
        ]
        project_catalog_sources: list[SimpleNamespace] = [
            _make_wrapper(
                unique_id=k,
                original_file_path=clean_path_str(sources_dict[k]["original_file_path"])
                if k in sources_dict
                else "",
                catalog_source=DictProxy(v),
            )
            for k, v in catalog_dict.get("sources", {}).items()
            if k.split(".")[1] == target_package
        ]
    else:
        project_catalog_nodes = []
        project_catalog_sources = []

    # --- Run Results ---
    if (
        hasattr(bouncer_config, "run_results_checks")
        and bouncer_config.run_results_checks != []
    ):
        rr_path = dbt_artifacts_dir / "run_results.json"
        if not rr_path.exists():
            raise FileNotFoundError(f"No run_results.json found at {rr_path}.")

        rr_dict = orjson.loads(rr_path.read_bytes())
        nodes_dict = manifest_dict.get("nodes", {})
        exposures_dict = manifest_dict.get("exposures", {})
        unit_tests_dict = manifest_dict.get("unit_tests", {})

        project_run_results: list[SimpleNamespace] = []
        for r in rr_dict.get("results", []):
            uid = r.get("unique_id", "")
            if uid.split(".")[1] != target_package:
                continue
            # Resolve original_file_path from manifest
            if uid in nodes_dict:
                ofp = clean_path_str(nodes_dict[uid].get("original_file_path", ""))
            elif uid.startswith("exposure.") and uid in exposures_dict:
                ofp = clean_path_str(exposures_dict[uid].get("original_file_path", ""))
            elif uid in unit_tests_dict:
                ofp = clean_path_str(unit_tests_dict[uid].get("original_file_path", ""))
            else:
                ofp = ""
            project_run_results.append(
                _make_wrapper(
                    unique_id=uid,
                    original_file_path=ofp,
                    run_result=DictProxy(r),
                )
            )
    else:
        project_run_results = []

    # Log parsed counts
    _log_artifact_summary(
        bouncer_config=bouncer_config,
        target_package=target_package,
        project_exposures=project_exposures,
        project_macros=project_macros,
        project_models=project_models,
        project_seeds=project_seeds,
        project_semantic_models=project_semantic_models,
        project_snapshots=project_snapshots,
        project_sources=project_sources,
        project_tests=project_tests,
        project_unit_tests=project_unit_tests,
        project_catalog_nodes=project_catalog_nodes,
        project_catalog_sources=project_catalog_sources,
        project_run_results=project_run_results,
    )

    return ParsedArtifacts(
        manifest_obj=manifest_obj,
        exposures=project_exposures,
        macros=project_macros,
        models=project_models,
        seeds=project_seeds,
        semantic_models=project_semantic_models,
        snapshots=project_snapshots,
        sources=project_sources,
        tests=project_tests,
        unit_tests=project_unit_tests,
        catalog_nodes=project_catalog_nodes,
        catalog_sources=project_catalog_sources,
        run_results=project_run_results,
    )


def _log_artifact_summary(
    bouncer_config: DbtBouncerConfBase,
    target_package: str,
    project_exposures: list[Any],
    project_macros: list[Any],
    project_models: list[Any],
    project_seeds: list[Any],
    project_semantic_models: list[Any],
    project_snapshots: list[Any],
    project_sources: list[Any],
    project_tests: list[Any],
    project_unit_tests: list[Any],
    project_catalog_nodes: list[Any],
    project_catalog_sources: list[Any],
    project_run_results: list[Any],
) -> None:
    """Log a summary table of parsed artifacts."""
    from rich import box
    from rich.console import Console
    from rich.table import Table

    table = Table(
        title=f"[bold cyan]Parsed artifacts for '{target_package}'[/bold cyan]",
        show_header=True,
        header_style="bold cyan",
        box=box.ROUNDED,
        border_style="cyan",
    )
    table.add_column("Artifact", justify="left", style="dim")
    table.add_column("Category", justify="left", style="bright_white")
    table.add_column("Count", justify="right", style="bold green")

    table.add_row("manifest.json", "Exposures", str(len(project_exposures)))
    table.add_row("", "Macros", str(len(project_macros)))
    table.add_row("", "Nodes", str(len(project_models)))
    table.add_row("", "Seeds", str(len(project_seeds)))
    table.add_row("", "Semantic Models", str(len(project_semantic_models)))
    table.add_row("", "Snapshots", str(len(project_snapshots)))
    table.add_row("", "Sources", str(len(project_sources)))
    table.add_row("", "Tests", str(len(project_tests)))
    table.add_row("", "Unit Tests", str(len(project_unit_tests)))

    if (
        hasattr(bouncer_config, "catalog_checks")
        and bouncer_config.catalog_checks != []
    ):
        table.add_row("catalog.json", "Nodes", str(len(project_catalog_nodes)))
        table.add_row("", "Sources", str(len(project_catalog_sources)))

    if (
        hasattr(bouncer_config, "run_results_checks")
        and bouncer_config.run_results_checks != []
    ):
        table.add_row("run_results.json", "Results", str(len(project_run_results)))

    string_io = io.StringIO()
    console = Console(file=string_io, force_terminal=False)
    console.print(table)
    table_str = string_io.getvalue().rstrip()

    logging.info(f"\n{table_str}")
