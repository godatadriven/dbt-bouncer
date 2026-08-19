"""Assemble and run all checks."""

import operator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict

from dbt_bouncer.executor import Executor
from dbt_bouncer.reporting.reporter import Reporter
from dbt_bouncer.utils import (
    clean_path_str,
    get_nested_value,
    object_excluded_by_path,
    object_in_path,
)

if TYPE_CHECKING:
    from dbt_bouncer.context import BouncerContext


# Maps each check class to its iterate-over resource name (an empty frozenset
# for context-only checks), set by the ``@check`` decorator. Memoised here --
# rather than re-reading the ClassVar per check -- because the same mapping is
# also handed to the dry-run reporter for its "Resource type" column.
_CLASS_ITERATE_CACHE: dict[type, frozenset[str]] = {}


@dataclass(slots=True)
class _ResourceFacts:
    """Per-resource values reused by every check that targets the resource.

    All of these are functions of the resource alone, so matching computes them
    once per resource rather than once per (check, resource) pair.
    """

    resource: Any
    skip_checks: list[str]
    run_id_suffix: str
    file_path: str | None
    unique_id: str | None
    cleaned_path: str


def _pattern_key(pattern: str | list[str] | None) -> Any:
    """Return a hashable cache key for an include/exclude pattern.

    Returns:
        Any: The pattern itself when hashable, else a tuple of its items.

    """
    return tuple(pattern) if isinstance(pattern, list) else pattern


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


def _run_id_suffix(resource: Any, iterate_value: str) -> str:
    """Build the resource half of a check-run ID.

    Depends only on the resource, so matching caches it per resource instead of
    recomputing it for every check that matches.

    Returns:
        str: The resource suffix (e.g. "staging_crm_stg_customers").

    """
    match iterate_value:
        case "exposure" | "macro" | "test" | "unit_test":
            return resource.unique_id.split(".")[-1]
        case _:
            return "_".join(getattr(resource, iterate_value).unique_id.split(".")[2:])


class CheckToRun(TypedDict):
    """A single check ready for execution, with its run context.

    ``check`` is a shared check-config instance; ``resource`` and ``iterate_value``
    tell the executor which resource to bind onto it immediately before calling
    ``execute()``. Because checks run sequentially, one instance can serve all of
    its resources in turn — no per-resource copy is needed.
    """

    check: Any
    check_run_id: str
    failure_message: NotRequired[str]
    file_path: NotRequired[str | None]
    iterate_value: NotRequired[str | None]
    outcome: NotRequired[str]
    resource: NotRequired[Any]
    severity: str
    unique_id: NotRequired[str | None]


def _check_applies_to_resource(
    check_name: str,
    check_code: str | None,
    materialization: str | None,
    facts: "_ResourceFacts",
) -> bool:
    """Decide whether a check runs against a resource, ignoring path filters.

    Path include/exclude is handled separately (and memoised per pattern pair)
    because it depends only on the pattern and the path; what remains is the
    per-pair part: the materialization filter and the resource's own
    ``skip_checks`` meta.

    Args:
        check_name: The check's configured name.
        check_code: The check's rule code, if it has one.
        materialization: The required materialization, or ``None`` when the
            check does not filter on it (including every non-model resource).
        facts: The resource's precomputed facts.

    Returns:
        bool: Whether the check should run against this resource.

    """
    if (
        materialization is not None
        and materialization != facts.resource.model.config.materialized
    ):
        return False

    skip_checks = facts.skip_checks
    return not (
        skip_checks
        and (check_name in skip_checks or (check_code and check_code in skip_checks))
    )


# Underscore-prefixed as an internal helper, but imported by the benchmark suite
# (``tests/benchmark``) to time the match phase in isolation — keep it importable.
def _assemble_checks_to_run(ctx: "BouncerContext") -> list[CheckToRun]:
    """Match checks to resources and build the run list.

    Builds the check context and per-resource skip-checks lookups, then iterates
    every configured check, matching it against the relevant resources and
    recording the shared check instance plus the resource to bind onto it per
    match. The executor binds the resource immediately before executing, so no
    per-resource copy is made.

    Returns:
        list[CheckToRun]: The assembled checks, ready for execution.

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

    from dbt_bouncer.check_framework.context import CheckContext

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

    # Per-iterate_value cache of per-resource facts. Every field depends only on
    # the resource, never on the check, so computing them once per resource
    # (instead of once per (check, resource) pair) is a big win when the config
    # has many checks targeting the same resource type: the run-ID suffix, file
    # path and unique_id are all string work over proxy objects, and
    # ``skip_checks`` is a nested meta lookup.
    resources_with_meta: dict[str, list[_ResourceFacts]] = {}

    def _resources_for(iterate_value: str) -> list[_ResourceFacts]:
        cached = resources_with_meta.get(iterate_value)
        if cached is not None:
            return cached
        out: list[_ResourceFacts] = []
        for resource in resource_map[f"{iterate_value}s"]:
            d = _get_resource_meta(resource, iterate_value, meta_by_unique_id)
            file_path = getattr(resource, "original_file_path", None)
            out.append(
                _ResourceFacts(
                    resource=resource,
                    skip_checks=get_nested_value(d, ["dbt-bouncer", "skip_checks"], []),
                    run_id_suffix=_run_id_suffix(resource, iterate_value),
                    file_path=file_path,
                    unique_id=getattr(resource, "unique_id", None),
                    cleaned_path=clean_path_str(file_path or ""),
                )
            )
        resources_with_meta[iterate_value] = out
        return out

    # Memoised selector resolution. A selector resolves to a static set of
    # unique IDs for the manifest, so checks sharing a selector string reuse
    # one resolved instance.
    selectors_by_raw: dict[str, Any] = {}

    def _selector_for(raw: str) -> Any:
        cached = selectors_by_raw.get(raw)
        if cached is not None:
            return cached
        from dbt_bouncer.selectors import Selector

        selector = Selector(raw, ctx.manifest_obj.manifest)
        selectors_by_raw[raw] = selector
        return selector

    # Memoised include/exclude/selector filtering. Matching depends only on the
    # check's patterns and the resource, so checks sharing a pattern triple
    # (very common -- most set none) reuse one filtered list instead of
    # re-running the regexes per check.
    path_filtered: dict[tuple[str, Any, Any, Any], list[_ResourceFacts]] = {}

    def _path_filtered_for(check: Any, iterate_value: str) -> list[_ResourceFacts]:
        include, exclude = check.include, check.exclude
        selector_raw = getattr(check, "selector", None)
        key = (
            iterate_value,
            _pattern_key(include),
            _pattern_key(exclude),
            selector_raw,
        )
        cached = path_filtered.get(key)
        if cached is not None:
            return cached
        candidates = _resources_for(iterate_value)
        if include is None and not exclude:
            # Purely an allocation optimisation, not a correctness guard:
            # ``object_in_path`` already returns True for a ``None`` include and
            # ``object_excluded_by_path`` returns False for an absent exclude, so
            # the comprehension below would produce this same list. Skipping it
            # avoids copying the list for the common case of a check that filters
            # on neither.
            result = candidates
        else:
            result = [
                facts
                for facts in candidates
                if object_in_path(include, facts.cleaned_path)
                and not object_excluded_by_path(exclude, facts.cleaned_path)
            ]
        if selector_raw:
            selector = _selector_for(selector_raw)
            result = [facts for facts in result if selector.matches(facts.unique_id)]
        path_filtered[key] = result
        return result

    checks_to_run: list[CheckToRun] = []
    for check in sorted(list_of_check_configs, key=operator.attrgetter("index")):
        cls = check.__class__
        if cls not in _CLASS_ITERATE_CACHE:
            # Set by the @check decorator; None for context-only checks.
            explicit = getattr(cls, "iterate_over", None)
            _CLASS_ITERATE_CACHE[cls] = (
                frozenset({explicit}) if explicit is not None else frozenset()
            )
        iterate_over_value = _CLASS_ITERATE_CACHE[cls]
        if iterate_over_value:
            iterate_value = next(iter(iterate_over_value))
            # The context is identical for every resource, so set it once on the
            # shared check-config instance rather than per match.
            check.set_context(check_ctx)
            check_name = check.name
            check_code = getattr(check, "code", None)
            severity = check.severity
            id_prefix = f"{check_name}:{check.index}:"
            materialization = (
                check.materialization if iterate_value == "model" else None
            )
            for facts in _path_filtered_for(check, iterate_value):
                if not _check_applies_to_resource(
                    check_name, check_code, materialization, facts
                ):
                    continue
                # No per-resource copy: the executor binds ``resource`` onto the
                # shared ``check`` instance immediately before calling execute().
                # Checks run sequentially, so reusing one instance across all of
                # its resources is safe (and avoids ~17k+ model_copy calls).
                checks_to_run.append(
                    {
                        "check": check,
                        "check_run_id": id_prefix + facts.run_id_suffix,
                        "file_path": facts.file_path,
                        "iterate_value": iterate_value,
                        "resource": facts.resource,
                        "severity": severity,
                        "unique_id": facts.unique_id,
                    },
                )
        else:
            check_run_id = f"{check.name}:{check.index}"
            check.set_context(check_ctx)
            checks_to_run.append(
                {
                    "check": check,
                    "check_run_id": check_run_id,
                    "file_path": None,
                    "severity": check.severity,
                    "unique_id": None,
                },
            )

    return checks_to_run


def runner(
    ctx: "BouncerContext",
) -> tuple[int, list[Any]]:
    """Run dbt-bouncer checks.

    Returns:
        tuple[int, list[Any]]: A tuple containing the exit code and a list of failed checks.

    """
    checks_to_run = _assemble_checks_to_run(ctx)

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
