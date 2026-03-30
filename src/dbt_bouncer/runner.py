"""Assemble and run all checks."""

import operator
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict

from dbt_bouncer.enums import ResourceType
from dbt_bouncer.executor import Executor
from dbt_bouncer.reporting.reporter import Reporter
from dbt_bouncer.utils import get_nested_value, resource_in_path

if TYPE_CHECKING:
    from dbt_bouncer.context import BouncerContext


_VALID_ITERATE_OVER_VALUES = frozenset(rt.value for rt in ResourceType)
_CLASS_ITERATE_CACHE: dict[type, frozenset[str]] = {}


def _get_resource_meta(
    resource: Any,
    iterate_value: str,
    meta_by_unique_id: dict[str, Any],
) -> dict[str, Any]:
    """Extract the dbt-bouncer meta config for a resource.

    Different resource types store their meta config in different locations.
    This helper centralises that per-type logic.

    Args:
        resource: The wrapper resource object (e.g. ModelWrapper).
        iterate_value: The singular resource type name (e.g. "model", "test").
        meta_by_unique_id: Pre-built mapping of unique_id -> meta for catalog nodes.

    Returns:
        dict[str, Any]: The meta dict for the resource, or {} if not applicable.

    """
    if iterate_value in {"model", "seed", "semantic_model", "snapshot", "source"}:
        try:
            return getattr(resource, iterate_value).config.meta or {}
        except AttributeError:
            return getattr(resource, iterate_value).meta or {}
    elif iterate_value == "catalog_node":
        return meta_by_unique_id.get(getattr(resource, "unique_id", ""), {})
    elif iterate_value == "run_result":
        return {}
    elif iterate_value == "macro":
        return resource.meta or {}
    elif iterate_value == "test":
        return getattr(getattr(resource, "test", resource), "meta", {}) or {}
    else:
        try:
            return resource.config.meta or {}
        except AttributeError:
            return resource.meta or {}


def _build_check_run_id(check: Any, resource: Any, iterate_value: str) -> str:
    """Build a unique run ID string for a check against a specific resource.

    Args:
        check: The check instance (must have .name and .index attributes).
        resource: The wrapper resource object.
        iterate_value: The singular resource type name (e.g. "model", "exposure").

    Returns:
        str: The check run ID in the format "check_name:index:resource_suffix".

    """
    match iterate_value:
        case "exposure" | "macro" | "test" | "unit_test":
            suffix = resource.unique_id.split(".")[-1]
        case _:
            suffix = "_".join(getattr(resource, iterate_value).unique_id.split(".")[2:])
    return f"{check.name}:{check.index}:{suffix}"


class CheckToRun(TypedDict):
    """A single check instance ready for execution, with its run context."""

    check: Any
    check_run_id: str
    failure_message: NotRequired[str]
    outcome: NotRequired[str]
    severity: str


def _should_run_check(
    check: Any,
    resource: Any,
    iterate_over_value: frozenset[str],
    meta_config: list[str],
) -> bool:
    """Determine if a check should run against a given resource.

    Evaluates three conditions:
    1. The resource path matches the check's include/exclude patterns.
    2. For model checks, the materialization matches (if specified).
    3. The check is not listed in the resource's skip_checks meta config.

    Returns:
        bool: Whether the check should run.

    """
    if not resource_in_path(check, resource):
        return False

    if (
        iterate_over_value == {"model"}
        and check.materialization is not None
        and check.materialization != resource.model.config.materialized
    ):
        return False

    return not (meta_config and check.name in meta_config)


def runner(
    ctx: "BouncerContext",
) -> tuple[int, list[Any]]:
    """Run dbt-bouncer checks.

    Returns:
        tuple[int, list[Any]]: A tuple containing the exit code and a list of failed checks.

    Raises:
        RuntimeError: If more than one "iterate_over" argument is found.

    """
    # resource_map: wrapper objects used for check iteration.
    # Keys that are already plain lists (catalog_nodes, catalog_sources, exposures,
    # macros, sources, unit_tests) are identical in both dicts; the others differ
    # because parsed_data stores unwrapped inner objects for context injection.
    resource_map: dict[str, list[Any]] = {
        "catalog_nodes": ctx.catalog_nodes,
        "catalog_sources": ctx.catalog_sources,
        "exposures": ctx.exposures,
        "macros": ctx.macros,
        "models": ctx.models,
        "run_results": ctx.run_results,
        "seeds": ctx.seeds,
        "semantic_models": ctx.semantic_models,
        "snapshots": ctx.snapshots,
        "sources": ctx.sources,
        "tests": ctx.tests,
        "unit_tests": ctx.unit_tests,
    }

    from dbt_bouncer.check_context import CheckContext

    check_ctx = CheckContext(
        catalog_nodes=ctx.catalog_nodes,
        catalog_sources=ctx.catalog_sources,
        exposures=ctx.exposures,
        exposures_by_unique_id=ctx.exposures_by_unique_id,
        macros=ctx.macros,
        manifest_obj=ctx.manifest_obj,
        models=ctx.models_flat,
        models_by_unique_id=ctx.models_by_unique_id,
        run_results=ctx.run_results_flat,
        seeds=ctx.seeds_flat,
        semantic_models=ctx.semantic_models_flat,
        snapshots=ctx.snapshots_flat,
        sources=ctx.sources,
        sources_by_unique_id=ctx.sources_by_unique_id,
        tests=ctx.tests_flat,
        tests_by_unique_id=ctx.tests_by_unique_id,
        unit_tests=ctx.unit_tests,
    )

    # Pre-compute unique_id -> meta lookup for catalog_node skip_checks
    meta_by_unique_id: dict[str, Any] = {}
    for resource_key in ["models", "seeds", "snapshots"]:
        for resource in resource_map.get(resource_key, []):
            inner_attr = resource_key.rstrip("s")  # "models" -> "model"
            node = getattr(resource, inner_attr, None)
            if node is not None and hasattr(node, "unique_id"):
                try:
                    meta_by_unique_id[node.unique_id] = node.config.meta
                except AttributeError:
                    meta_by_unique_id[node.unique_id] = getattr(node, "meta", {})

    list_of_check_configs = []
    for check_category in ctx.check_categories:
        list_of_check_configs.extend(getattr(ctx.bouncer_config, check_category))

    checks_to_run: list[CheckToRun] = []
    for check in sorted(list_of_check_configs, key=operator.attrgetter("index")):
        cls = check.__class__
        if cls not in _CLASS_ITERATE_CACHE:
            # Prefer explicit iterate_over ClassVar (set by @check decorator)
            # over annotation introspection.
            explicit = getattr(cls, "iterate_over", None)
            if explicit is not None:
                _CLASS_ITERATE_CACHE[cls] = frozenset({explicit})
            else:
                _CLASS_ITERATE_CACHE[cls] = _VALID_ITERATE_OVER_VALUES.intersection(
                    frozenset(cls.__annotations__.keys()),
                )
        iterate_over_value = _CLASS_ITERATE_CACHE[cls]
        if len(iterate_over_value) == 1:
            iterate_value = next(iter(iterate_over_value))
            for i in resource_map[f"{iterate_value}s"]:
                check_i = check.model_copy(deep=True)
                d = _get_resource_meta(i, iterate_value, meta_by_unique_id)
                meta_config = get_nested_value(
                    d,
                    ["dbt-bouncer", "skip_checks"],
                    [],
                )
                if _should_run_check(check_i, i, iterate_over_value, meta_config):
                    check_run_id = _build_check_run_id(check_i, i, iterate_value)
                    check_i.set_resource(i, iterate_value)
                    check_i.set_context(check_ctx)

                    checks_to_run.append(
                        {
                            "check": check_i,
                            "check_run_id": check_run_id,
                            "severity": check_i.severity,
                        },
                    )
        elif len(iterate_over_value) > 1:
            raise RuntimeError(
                f"Check {check.name} has multiple iterate_over_value values: {iterate_over_value}",
            )
        else:
            check_run_id = f"{check.name}:{check.index}"
            check.set_context(check_ctx)
            checks_to_run.append(
                {
                    "check": check,
                    "check_run_id": check_run_id,
                    "severity": check.severity,
                },
            )

    del (
        ctx.models,
        ctx.run_results,
        ctx.seeds,
        ctx.semantic_models,
        ctx.snapshots,
        ctx.tests,
    )

    reporter = Reporter(
        show_all_failures=ctx.show_all_failures,
        create_pr_comment_file=ctx.create_pr_comment_file,
        output_file=ctx.output_file,
        output_format=ctx.output_format,
        output_only_failures=ctx.output_only_failures,
    )

    if ctx.dry_run:
        return reporter.report_dry_run(
            checks_to_run, iterate_cache=_CLASS_ITERATE_CACHE
        )

    executor = Executor()
    results = executor.run(checks_to_run)

    return reporter.report_results(results)
