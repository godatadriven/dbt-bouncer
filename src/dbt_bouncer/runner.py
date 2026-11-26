"""Assemble and run all checks."""

import logging
import operator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, NotRequired, TypedDict

from dbt_bouncer.enums import ExitCode
from dbt_bouncer.executor import Executor
from dbt_bouncer.reporting.reporter import Reporter
from dbt_bouncer.utils import clean_path_str, compile_pattern

if TYPE_CHECKING:
    from collections.abc import Callable, Collection

    from dbt_bouncer.context import BouncerContext


# Maps each check class to its iterate-over resource name (an empty frozenset
# for context-only checks), set by the ``@check`` decorator. Memoised here --
# rather than re-reading the ClassVar per check -- because the same mapping is
# also handed to the dry-run reporter for its "Resource type" column.
_CLASS_ITERATE_CACHE: dict[type, frozenset[str]] = {}

_EMPTY_SKIP: frozenset[str] = frozenset()
_WRAPPED_RESOURCE_TYPES = frozenset(
    {"model", "seed", "semantic_model", "snapshot", "source"}
)


@dataclass(slots=True)
class _ResourceFacts:
    """Per-resource values reused by every check that targets the resource.

    All of these are functions of the resource alone, so matching computes them
    once per resource rather than once per (check, resource) pair.
    """

    resource: Any
    skip_checks: "Collection[str]"
    run_id_suffix: str
    file_path: str | None
    unique_id: str | None
    cleaned_path: str
    inner_resource: Any = None


def _pattern_key(pattern: str | list[str] | None) -> Any:
    """Return a hashable cache key for an include/exclude pattern.

    Returns:
        Any: The pattern itself when hashable, else a tuple of its items.

    """
    return tuple(pattern) if isinstance(pattern, list) else pattern


def _compile_path_matchers(
    pattern: str | list[str] | None,
) -> "tuple[Callable[[str], Any], ...]":
    """Pre-compile an include/exclude pattern (or list) into bound ``.match`` callables.

    Compiling once per distinct filter avoids repeated ``lru_cache`` lookups and
    list-wrapping inside the per-resource loop.

    Returns:
        tuple[Callable[[str], Any], ...]: Bound regex matchers (empty when no filter).

    """
    if pattern is None:
        return ()
    patterns = (pattern,) if isinstance(pattern, str) else pattern
    return tuple(compile_pattern(p.strip()).match for p in patterns)


def _extract_skip_checks(meta: dict[str, Any]) -> frozenset[str]:
    """Extract ``meta['dbt-bouncer']['skip_checks']`` as a frozenset for O(1) lookup.

    Uses raw ``dict.get`` when ``meta`` is a dict to avoid ``DictProxy``/``ListProxy``
    wrapping overhead on the hot matching path.

    Returns:
        frozenset[str]: Check names/codes to skip, or an empty frozenset.

    """
    if not meta:
        return _EMPTY_SKIP
    bouncer_meta = dict.get(meta, "dbt-bouncer")
    if not isinstance(bouncer_meta, dict):
        return _EMPTY_SKIP
    raw_skip = dict.get(bouncer_meta, "skip_checks")
    return frozenset(raw_skip) if raw_skip else _EMPTY_SKIP


def _get_resource_meta(
    resource: Any,
    iterate_value: str,
    meta_by_unique_id: dict[str, Any],
) -> dict[str, Any]:
    """Extract the dbt-bouncer meta config for a resource.

    Different resource types store their meta config in different locations.
    When the underlying node is a ``dict`` (including ``DictProxy``), raw
    ``dict.get`` is used so matching does not allocate nested proxy copies.

    Args:
        resource: The wrapper resource object (e.g. ModelWrapper).
        iterate_value: The singular resource type name (e.g. "model", "test").
        meta_by_unique_id: Pre-built mapping of unique_id -> meta for catalog nodes.

    Returns:
        dict[str, Any]: The meta dict for the resource, or {} if not applicable.

    """
    if iterate_value == "run_result":
        return {}
    if iterate_value in {"catalog_node", "catalog_source"}:
        return meta_by_unique_id.get(getattr(resource, "unique_id", ""), {})

    if iterate_value in _WRAPPED_RESOURCE_TYPES:
        node = getattr(resource, iterate_value)
    elif iterate_value == "test":
        node = getattr(resource, "test", resource)
    else:
        node = resource

    if isinstance(node, dict):
        if iterate_value in {"macro", "test"}:
            return dict.get(node, "meta") or {}
        cfg = dict.get(node, "config")
        if isinstance(cfg, dict):
            return dict.get(cfg, "meta") or {}
        return dict.get(node, "meta") or {}

    try:
        return node.config.meta or {}
    except AttributeError:
        return getattr(node, "meta", None) or {}


def _run_id_suffix(resource: Any, iterate_value: str) -> str:
    """Build the resource half of a check-run ID.

    Depends only on the resource, so matching caches it per resource instead of
    recomputing it for every check that matches.

    Returns:
        str: The resource suffix (e.g. "staging_crm_stg_customers").

    """
    uid = (
        dict.get(resource, "unique_id")
        if isinstance(resource, dict)
        else getattr(resource, "unique_id", None)
    )
    if uid is None:
        inner = getattr(resource, iterate_value)
        uid = (
            dict.__getitem__(inner, "unique_id")
            if isinstance(inner, dict)
            else inner.unique_id
        )
    uid_str = str(uid)
    if iterate_value in {"exposure", "macro", "test", "unit_test"}:
        return uid_str.rsplit(".", 1)[-1]
    return "_".join(uid_str.split(".")[2:])


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
    inner_resource: NotRequired[Any]
    iterate_value: NotRequired[str | None]
    outcome: NotRequired[str]
    resource: NotRequired[Any]
    severity: str
    unique_id: NotRequired[str | None]


def _check_applies_to_resource(
    check_name: str,
    check_code: str | None,
    language: str | None,
    materialization: str | None,
    facts: "_ResourceFacts",
) -> bool:
    """Decide whether a check runs against a resource, ignoring path filters.

    Path include/exclude is handled separately (and memoised per pattern pair)
    because it depends only on the pattern and the path; what remains is the
    per-pair part: the language and materialization filters and the resource's
    own ``skip_checks`` meta.

    Args:
        check_name: The check's configured name.
        check_code: The check's rule code, if it has one.
        language: The required model language, or ``None`` when the check does
            not filter on it (including every non-model resource).
        materialization: The required materialization, or ``None`` when the
            check does not filter on it (including every non-model resource).
        facts: The resource's precomputed facts.

    Returns:
        bool: Whether the check should run against this resource.

    """
    # `language` is optional in the manifest schema: treat a missing value as SQL.
    if language is not None and language != (facts.resource.model.language or "sql"):
        return False

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
        info_schema=ctx.info_schema,
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
    attribute, so unwrap it before reading meta. Uses raw ``dict.get`` for
    ``DictProxy`` nodes to avoid copying nested config/meta dicts.

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
            if node is None:
                continue
            if isinstance(node, dict):
                uid = dict.get(node, "unique_id")
                if uid is None:
                    continue
                cfg = dict.get(node, "config")
                meta_by_unique_id[uid] = (
                    dict.get(cfg, "meta")
                    if isinstance(cfg, dict)
                    else dict.get(node, "meta", {})
                )
            elif hasattr(node, "unique_id"):
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
    per-resource facts (run-ID suffix, file path, unique_id, inner resource,
    and the nested ``skip_checks`` meta lookup) are computed once per resource
    rather than once per (check, resource) pair.
    """

    def __init__(
        self,
        resource_map: dict[str, list[Any]],
        meta_by_unique_id: dict[str, Any] | None,
        manifest_obj: Any,
    ) -> None:
        self._resource_map = resource_map
        self._meta_by_unique_id = meta_by_unique_id
        self._manifest_obj = manifest_obj
        self._resources_with_meta: dict[str, list[_ResourceFacts]] = {}
        self._has_skip_checks: dict[str, bool] = {}
        self._selectors_by_raw: dict[str, Any] = {}
        self._selector_resources: list[tuple[str, Any]] | None = None
        self._path_filtered: dict[tuple[str, Any, Any, Any], list[_ResourceFacts]] = {}

    def _get_meta_by_unique_id(self) -> dict[str, Any]:
        """Return the catalog unique_id -> meta map, building it lazily on first use.

        Returns:
            dict[str, Any]: Mapping of unique_id to meta dict.

        """
        if self._meta_by_unique_id is None:
            self._meta_by_unique_id = _build_meta_by_unique_id(self._resource_map)
        return self._meta_by_unique_id

    def has_skip_checks(self, iterate_value: str) -> bool:
        """Whether any resource of ``iterate_value`` declares ``skip_checks``.

        Returns:
            bool: ``True`` if at least one resource has non-empty ``skip_checks``.

        """
        return self._has_skip_checks.get(iterate_value, False)

    def resources_for(self, iterate_value: str) -> list[_ResourceFacts]:
        """Return the cached per-resource facts for a resource type.

        Returns:
            list[_ResourceFacts]: One entry per resource of the given type.

        """
        cached = self._resources_with_meta.get(iterate_value)
        if cached is not None:
            return cached
        meta_map = (
            self._get_meta_by_unique_id()
            if iterate_value in {"catalog_node", "catalog_source"}
            else {}
        )
        out: list[_ResourceFacts] = []
        any_skip = False
        for resource in self._resource_map[f"{iterate_value}s"]:
            d = _get_resource_meta(resource, iterate_value, meta_map)
            skip_checks = _extract_skip_checks(d)
            if skip_checks:
                any_skip = True
            if isinstance(resource, dict):
                file_path = dict.get(resource, "original_file_path")
                unique_id = dict.get(resource, "unique_id")
                inner_resource = dict.get(resource, iterate_value, resource)
            else:
                file_path = getattr(resource, "original_file_path", None)
                unique_id = getattr(resource, "unique_id", None)
                inner_resource = getattr(resource, iterate_value, resource)
            cleaned_path = (
                clean_path_str(file_path)
                if file_path and "\\" in file_path
                else (file_path or "")
            )
            out.append(
                _ResourceFacts(
                    resource=resource,
                    skip_checks=skip_checks,
                    run_id_suffix=_run_id_suffix(resource, iterate_value),
                    file_path=file_path,
                    unique_id=unique_id,
                    cleaned_path=cleaned_path,
                    inner_resource=inner_resource,
                )
            )
        self._resources_with_meta[iterate_value] = out
        self._has_skip_checks[iterate_value] = any_skip
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

        manifest = self._manifest_obj.manifest
        if self._selector_resources is None:
            self._selector_resources = list(Selector._iter_manifest_resources(manifest))
        selector = Selector(raw, manifest, _resources=self._selector_resources)
        self._selectors_by_raw[raw] = selector
        return selector

    def path_filtered_for(self, check: Any, iterate_value: str) -> list[_ResourceFacts]:
        """Return the resources a check runs against after include/exclude/selector.

        Matching depends only on the check's patterns and the resource, so checks
        sharing a pattern triple (very common -- most set none) reuse one filtered
        list instead of re-running the regexes per check. Regex patterns are
        pre-compiled once per distinct filter rather than looked up per resource.

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
        inc_matchers = _compile_path_matchers(include)
        exc_matchers = _compile_path_matchers(exclude)
        if not inc_matchers and not exc_matchers:
            result = candidates
        elif inc_matchers and not exc_matchers:
            if len(inc_matchers) == 1:
                m0 = inc_matchers[0]
                result = [f for f in candidates if m0(f.cleaned_path) is not None]
            else:
                result = [
                    f
                    for f in candidates
                    if any(m(f.cleaned_path) is not None for m in inc_matchers)
                ]
        elif not inc_matchers:
            result = [
                f
                for f in candidates
                if not any(m(f.cleaned_path) is not None for m in exc_matchers)
            ]
        else:
            result = [
                f
                for f in candidates
                if any(m(f.cleaned_path) is not None for m in inc_matchers)
                and not any(m(f.cleaned_path) is not None for m in exc_matchers)
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

    No per-resource copy is made: the executor binds ``inner_resource`` onto the
    shared ``check`` instance immediately before calling execute(). Checks run
    sequentially, so reusing one instance across its resources is safe (and avoids
    ~17k+ model_copy calls).

    Returns:
        list[CheckToRun]: One entry per matched resource.

    """
    check_name = check.name
    check_code = getattr(check, "code", None)
    severity = check.severity
    id_prefix = f"{check_name}:{check.index}:"
    is_model = iterate_value == "model"
    language = check.language if is_model else None
    materialization = check.materialization if is_model else None
    filtered = matcher.path_filtered_for(check, iterate_value)

    if (
        language is None
        and materialization is None
        and not matcher.has_skip_checks(iterate_value)
    ):
        return [
            {
                "check": check,
                "check_run_id": id_prefix + facts.run_id_suffix,
                "file_path": facts.file_path,
                "inner_resource": facts.inner_resource,
                "iterate_value": iterate_value,
                "resource": facts.resource,
                "severity": severity,
                "unique_id": facts.unique_id,
            }
            for facts in filtered
        ]

    entries: list[CheckToRun] = []
    for facts in filtered:
        if not _check_applies_to_resource(
            check_name, check_code, language, materialization, facts
        ):
            continue
        entries.append(
            {
                "check": check,
                "check_run_id": id_prefix + facts.run_id_suffix,
                "file_path": facts.file_path,
                "inner_resource": facts.inner_resource,
                "iterate_value": iterate_value,
                "resource": facts.resource,
                "severity": severity,
                "unique_id": facts.unique_id,
            },
        )
    return entries


def _assemble_checks_to_run(ctx: "BouncerContext") -> list[CheckToRun]:
    """Match checks to resources and build the run list.

    Groups enabled checks by resource type (preserving index order within each
    group) so per-resource facts, compiled path patterns, selectors, and
    ``skip_checks`` lookups are computed once per resource type and reused
    across every check targeting that type.

    Returns:
        list[CheckToRun]: The assembled checks, ready for execution.

    """
    resource_map = _build_resource_map(ctx)
    check_ctx = _build_check_context(ctx)
    matcher = _CheckMatcher(resource_map, None, ctx.manifest_obj)

    list_of_check_configs = []
    for check_category in ctx.check_categories:
        list_of_check_configs.extend(getattr(ctx.bouncer_config, check_category))

    grouped: dict[str | None, list[Any]] = {}
    for check in sorted(list_of_check_configs, key=operator.attrgetter("index")):
        check.set_context(check_ctx)
        grouped.setdefault(_iterate_value_for(check.__class__), []).append(check)

    checks_to_run: list[CheckToRun] = []
    for iterate_value, checks in grouped.items():
        if iterate_value is not None:
            for check in checks:
                checks_to_run.extend(
                    _iterating_check_entries(check, iterate_value, matcher)
                )
        else:
            for check in checks:
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
        # Slim CI (`state:modified+`) legitimately produces artifacts with no
        # matching resources when a PR changes no models.
        if ctx.allow_zero_checks:
            logging.warning(
                "No checks were run. Exiting with code 0 because "
                "`--allow-zero-checks` is set."
            )
            return (ExitCode.SUCCESS, [])
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
