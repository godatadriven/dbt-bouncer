"""Assemble and run all checks."""

import logging
import operator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict

from dbt_bouncer.enums import ExitCode
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
    elif iterate_value in {"catalog_node", "catalog_source"}:
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
def _build_resource_map(ctx: "BouncerContext") -> dict[str, list[Any]]:
    """Map each iterate-over key to the wrapper objects used for check iteration.

    Keys that are already plain lists (catalog_nodes, catalog_sources, exposures,
    macros, sources, unit_tests) are identical to ``parsed_data``; the others differ
    because ``parsed_data`` stores unwrapped inner objects for context injection.

    Returns:
        dict[str, list[Any]]: The resource map keyed by iterate-over name.

    """
    return {
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


def _build_check_context(ctx: "BouncerContext") -> Any:
    """Build the shared ``CheckContext`` injected into every check.

    Returns:
        CheckContext: The context shared by all checks in this run.

    """
    from dbt_bouncer.check_framework.context import CheckContext

    return CheckContext(
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


def _build_meta_by_unique_id(resource_map: dict[str, list[Any]]) -> dict[str, Any]:
    """Pre-compute a unique_id -> meta lookup for skip_checks resolution.

    Each resource wraps the real node under a ``.source``/``.model``/etc.
    attribute, so unwrap it before reading meta.

    Returns:
        dict[str, Any]: Mapping of resource unique_id to its meta config.

    """
    inner_attr_by_key = {
        "models": "model",
        "seeds": "seed",
        "snapshots": "snapshot",
        "sources": "source",
    }
    meta_by_unique_id: dict[str, Any] = {}
    for resource_key, inner_attr in inner_attr_by_key.items():
        for resource in resource_map.get(resource_key, []):
            node = getattr(resource, inner_attr, None)
            if node is not None and hasattr(node, "unique_id"):
                try:
                    meta_by_unique_id[node.unique_id] = node.config.meta
                except AttributeError:
                    meta_by_unique_id[node.unique_id] = getattr(node, "meta", {})
    return meta_by_unique_id


class _CheckMatcher:
    """Resolves the resources each configured check runs against.

    Memoises per-iterate_value resource facts, resolved selectors, and
    include/exclude/selector filtering, so checks that share inputs reuse the
    work. When a config has many checks targeting the same resource type, the
    per-resource facts (run-ID suffix, file path, unique_id, and the nested
    ``skip_checks`` meta lookup) are computed once per resource rather than once
    per (check, resource) pair.
    """

    def __init__(
        self,
        resource_map: dict[str, list[Any]],
        meta_by_unique_id: dict[str, Any],
        manifest_obj: Any,
    ) -> None:
        self._resource_map = resource_map
        self._meta_by_unique_id = meta_by_unique_id
        self._manifest_obj = manifest_obj
        self._resources_with_meta: dict[str, list[_ResourceFacts]] = {}
        self._selectors_by_raw: dict[str, Any] = {}
        self._path_filtered: dict[tuple[str, Any, Any, Any], list[_ResourceFacts]] = {}

    def resources_for(self, iterate_value: str) -> list[_ResourceFacts]:
        """Return the cached per-resource facts for a resource type.

        Returns:
            list[_ResourceFacts]: One entry per resource of the given type.

        """
        cached = self._resources_with_meta.get(iterate_value)
        if cached is not None:
            return cached
        out: list[_ResourceFacts] = []
        for resource in self._resource_map[f"{iterate_value}s"]:
            d = _get_resource_meta(resource, iterate_value, self._meta_by_unique_id)
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
        self._resources_with_meta[iterate_value] = out
        return out

    def _selector_for(self, raw: str) -> Any:
        """Return the resolved (and cached) selector for a selector string.

        Returns:
            Selector: The resolved selector, reused across checks sharing ``raw``.

        """
        cached = self._selectors_by_raw.get(raw)
        if cached is not None:
            return cached
        from dbt_bouncer.selectors import Selector

        selector = Selector(raw, self._manifest_obj.manifest)
        self._selectors_by_raw[raw] = selector
        return selector

    def path_filtered_for(self, check: Any, iterate_value: str) -> list[_ResourceFacts]:
        """Return the resources a check runs against after include/exclude/selector.

        Matching depends only on the check's patterns and the resource, so checks
        sharing a pattern triple (very common -- most set none) reuse one filtered
        list instead of re-running the regexes per check.

        Returns:
            list[_ResourceFacts]: The resources the check runs against.

        """
        include, exclude = check.include, check.exclude
        selector_raw = check.selector
        key = (
            iterate_value,
            _pattern_key(include),
            _pattern_key(exclude),
            selector_raw,
        )
        cached = self._path_filtered.get(key)
        if cached is not None:
            return cached

        candidates = self.resources_for(iterate_value)
        if include is None and not exclude:
            # Purely an allocation optimisation, not a correctness guard:
            # ``object_in_path`` returns True for a ``None`` include and
            # ``object_excluded_by_path`` returns False for an absent exclude, so
            # the comprehension below would produce this same list. Skipping it
            # avoids copying the list for the common case that filters on neither.
            result = candidates
        else:
            result = [
                facts
                for facts in candidates
                if object_in_path(include, facts.cleaned_path)
                and not object_excluded_by_path(exclude, facts.cleaned_path)
            ]
        if selector_raw:
            selector = self._selector_for(selector_raw)
            result = [facts for facts in result if selector.matches(facts.unique_id)]
        self._path_filtered[key] = result
        return result


def _iterate_value_for(cls: type) -> str | None:
    """Return the single resource key a check class iterates over.

    Memoise the answer in ``_CLASS_ITERATE_CACHE`` (also read by the dry-run
    reporter). Return ``None`` for context-only checks.

    Returns:
        str | None: The iterate-over value, or ``None`` for context-only checks.

    """
    cached = _CLASS_ITERATE_CACHE.get(cls)
    if cached is None:
        # Set by the @check decorator; None for context-only checks.
        explicit = getattr(cls, "iterate_over", None)
        cached = frozenset({explicit}) if explicit is not None else frozenset()
        _CLASS_ITERATE_CACHE[cls] = cached
    return next(iter(cached)) if cached else None


def _iterating_check_entries(
    check: Any, iterate_value: str, matcher: "_CheckMatcher"
) -> list[CheckToRun]:
    """Build the run entries for a check that iterates over a resource type.

    No per-resource copy is made: the executor binds ``resource`` onto the shared
    ``check`` instance immediately before calling execute(). Checks run
    sequentially, so reusing one instance across its resources is safe (and avoids
    ~17k+ model_copy calls).

    Returns:
        list[CheckToRun]: One entry per matched resource.

    """
    check_name = check.name
    check_code = getattr(check, "code", None)
    severity = check.severity
    id_prefix = f"{check_name}:{check.index}:"
    materialization = check.materialization if iterate_value == "model" else None

    entries: list[CheckToRun] = []
    for facts in matcher.path_filtered_for(check, iterate_value):
        if not _check_applies_to_resource(
            check_name, check_code, materialization, facts
        ):
            continue
        entries.append(
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
    return entries


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
    resource_map = _build_resource_map(ctx)
    check_ctx = _build_check_context(ctx)
    meta_by_unique_id = _build_meta_by_unique_id(resource_map)
    matcher = _CheckMatcher(resource_map, meta_by_unique_id, ctx.manifest_obj)

    list_of_check_configs = []
    for check_category in ctx.check_categories:
        list_of_check_configs.extend(getattr(ctx.bouncer_config, check_category))

    checks_to_run: list[CheckToRun] = []
    for check in sorted(list_of_check_configs, key=operator.attrgetter("index")):
        # The context is identical for every resource, so set it once on the
        # shared check-config instance rather than per match.
        check.set_context(check_ctx)
        iterate_value = _iterate_value_for(check.__class__)
        if iterate_value is not None:
            checks_to_run.extend(
                _iterating_check_entries(check, iterate_value, matcher)
            )
        else:
            checks_to_run.append(
                {
                    "check": check,
                    "check_run_id": f"{check.name}:{check.index}",
                    "file_path": None,
                    "severity": check.severity,
                    "unique_id": None,
                },
            )

    return checks_to_run


def _release_ctx_resources(ctx: "BouncerContext") -> None:
    """Free the large per-resource lists once checks are assembled.

    The lists are emptied rather than deleted: this frees the wrapper objects
    they hold, avoids mutating Pydantic attribute metadata, and is safe to call
    from either entry point (`collect_failures` and `runner`).

    Only these six are freed: the check context consumes them in a derived form
    (the ``*_flat`` / ``*_by_unique_id`` cached properties), so the original
    wrappers are no longer needed. The remaining resource lists (exposures,
    macros, sources, unit_tests, catalog_nodes, catalog_sources) are passed to
    the check context directly and must stay.
    """
    ctx.models = []
    ctx.run_results = []
    ctx.seeds = []
    ctx.semantic_models = []
    ctx.snapshots = []
    ctx.tests = []


def collect_failures(ctx: "BouncerContext") -> list[dict[str, Any]]:
    """Assemble and execute checks, returning raw results without reporting.

    This is used to compute a baseline or a `--state` comparison set: it runs the
    checks but prints nothing and writes no output file.

    Returns:
        list[dict[str, Any]]: The executor result dicts. Empty if no checks ran.

    """
    checks_to_run = _assemble_checks_to_run(ctx)
    if not checks_to_run:
        return []

    _release_ctx_resources(ctx)
    return Executor().run(checks_to_run)


def runner(
    ctx: "BouncerContext",
    accept: set[str] | None = None,
) -> tuple[int, list[Any]]:
    """Run dbt-bouncer checks.

    Args:
        ctx: The execution context.
        accept: Fingerprints of already-known failures to suppress (baseline or
            state). Suppressed failures do not report and do not affect the exit
            code.

    Returns:
        tuple[int, list[Any]]: A tuple containing the exit code and a list of failed checks.

    """
    checks_to_run = _assemble_checks_to_run(ctx)

    # A config that matches no resources is almost always a mistake. Exit
    # non-zero so the mistake is not hidden by a green "all checks passed"
    # summary. The dry-run path is exempt; it reports the empty plan later.
    if not checks_to_run and not ctx.dry_run:
        logging.error(
            "No checks were run. A config that matches no resources exits without "
            "running anything. Check the `package_name`, the config file, the dbt "
            "artifacts, and any `--check` or `--only` filters."
        )
        return (ExitCode.NO_CHECKS_RUN, [])

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

    _release_ctx_resources(ctx)
    results = Executor().run(checks_to_run)

    if accept is not None:
        from dbt_bouncer.regression import apply_regression_filter

        results, _ = apply_regression_filter(results, accept)

    return reporter.report_results(results)
