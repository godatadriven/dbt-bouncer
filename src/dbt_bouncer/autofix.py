"""Automatic fixes for mechanically fixable check failures.

The scope is deliberately narrow and honest: a failure is only fixable when
the remedy is mechanical (no human judgement) and lives in a dbt properties
(YAML) file that the failing resource is already documented in. SQL files
are never touched, and no properties-file entry is ever created — only
existing entries are edited, via a round-trip YAML parser that preserves
comments and formatting.

Fixable checks:

- ``check_model_has_tags`` (``criteria: all`` only): append the missing
  tags to the model's ``config.tags``. dbt merges tags additively across
  sources, so appending is safe.
- ``check_model_access``: set the model's ``access`` to the required value.

Everything else is reported as not fixable, with a reason.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dbt_bouncer.enums import AutofixMutateResult

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from pathlib import Path


@dataclass(slots=True)
class PlannedFix:
    """One mechanically applicable fix, bound to a properties file."""

    check_run_id: str
    description: str
    file: Path
    mutate: Callable[[Any], AutofixMutateResult]


@dataclass(slots=True)
class SkippedFix:
    """One failure that could not be fixed, with the reason."""

    check_run_id: str
    reason: str


def resolve_patch_file(model: Any, project_dir: Path) -> Path | None:
    """Resolve a model's ``patch_path`` to a properties file on disk.

    Args:
        model: The manifest model node.
        project_dir: The dbt project root directory.

    Returns:
        Path | None: The properties file, or None when the model has no
        properties-file entry or the file does not exist.

    """
    patch_path = getattr(model, "patch_path", None)
    if not patch_path:
        return None
    # patch_path has the form "<project_name>://<relative_path>".
    relative = str(patch_path).split("://", 1)[-1]
    candidate = project_dir / relative
    return candidate if candidate.is_file() else None


def _find_model_entry(doc: Any, model_name: str) -> Any | None:
    """Find the entry for ``model_name`` in a loaded properties document.

    Returns:
        Any | None: The model's mapping entry, or None when absent.

    """
    for entry in doc.get("models") or []:
        if isinstance(entry, dict) and entry.get("name") == model_name:
            return entry
    return None


def _fix_model_has_tags(
    check: Any, model: Any, file: Path, check_run_id: str
) -> PlannedFix | SkippedFix:
    """Plan a fix that appends the missing tags to the model's config.

    Returns:
        PlannedFix | SkippedFix: The planned fix, or the reason it was skipped.

    """
    if str(getattr(check, "criteria", "all")) != "all":
        return SkippedFix(
            check_run_id=check_run_id,
            reason="criteria is not 'all'; choosing which tag to add is a judgement call",
        )

    existing = [str(t) for t in (getattr(model, "tags", None) or [])]
    missing = [t for t in check.tags if t not in existing]
    model_name = str(model.name)

    def mutate(doc: Any) -> AutofixMutateResult:
        entry = _find_model_entry(doc, model_name)
        if entry is None:
            return AutofixMutateResult.MISSING
        config = entry.setdefault("config", {})
        tags = config.setdefault("tags", [])
        changed = False
        for tag in missing:
            if tag not in tags:
                tags.append(tag)
                changed = True
        return AutofixMutateResult.CHANGED if changed else AutofixMutateResult.NOOP

    return PlannedFix(
        check_run_id=check_run_id,
        description=f"add tag(s) {missing} to `{model_name}`",
        file=file,
        mutate=mutate,
    )


def _fix_model_access(
    check: Any, model: Any, file: Path, check_run_id: str
) -> PlannedFix | SkippedFix:
    """Plan a fix that sets the model's access to the required value.

    Returns:
        PlannedFix | SkippedFix: The planned fix, or the reason it was skipped.

    """
    required = check.access
    required_str = str(getattr(required, "value", required))
    model_name = str(model.name)

    def mutate(doc: Any) -> AutofixMutateResult:
        entry = _find_model_entry(doc, model_name)
        if entry is None:
            return AutofixMutateResult.MISSING
        if entry.get("access") == required_str:
            return AutofixMutateResult.NOOP
        entry["access"] = required_str
        return AutofixMutateResult.CHANGED

    return PlannedFix(
        check_run_id=check_run_id,
        description=f"set `access: {required_str}` on `{model_name}`",
        file=file,
        mutate=mutate,
    )


_FIXERS: dict[str, Callable[[Any, Any, Path, str], PlannedFix | SkippedFix]] = {
    "check_model_access": _fix_model_access,
    "check_model_has_tags": _fix_model_has_tags,
}

FIXABLE_CHECK_NAMES = frozenset(_FIXERS)


def plan_fixes(
    failures: Sequence[Any], project_dir: Path
) -> tuple[list[PlannedFix], list[SkippedFix]]:
    """Plan fixes for a list of failed check results.

    Args:
        failures: Failed check results from the executor. Each carries the
            shared ``check`` instance and the matched ``resource``.
        project_dir: The dbt project root directory, used to resolve
            ``patch_path`` values.

    Returns:
        tuple[list[PlannedFix], list[SkippedFix]]: The applicable fixes, and
        the failures that could not be fixed with the reason for each.

    """
    planned: list[PlannedFix] = []
    skipped: list[SkippedFix] = []

    for failure in failures:
        check = failure["check"]
        check_run_id = failure["check_run_id"]
        fixer = _FIXERS.get(str(check.name))
        if fixer is None:
            skipped.append(
                SkippedFix(
                    check_run_id=check_run_id,
                    reason="no autofix is available for this check",
                )
            )
            continue

        resource = failure.get("resource")
        model = getattr(resource, "model", resource)
        file = resolve_patch_file(model, project_dir)
        if file is None:
            skipped.append(
                SkippedFix(
                    check_run_id=check_run_id,
                    reason="the model has no properties-file entry to edit",
                )
            )
            continue

        result = fixer(check, model, file, check_run_id)
        if isinstance(result, PlannedFix):
            planned.append(result)
        else:
            skipped.append(result)

    return planned, skipped


def apply_fixes(
    planned: list[PlannedFix], dry_run: bool = False
) -> tuple[list[PlannedFix], list[SkippedFix]]:
    """Apply planned fixes to their properties files.

    Fixes are grouped by file so each file is read and written once. The
    round-trip YAML parser preserves comments, quoting, and formatting.

    Args:
        planned: The fixes to apply.
        dry_run: When True, no file is written.

    Returns:
        tuple[list[PlannedFix], list[SkippedFix]]: The fixes that changed (or
        would change) a file, and the fixes whose mutation found nothing to
        edit.

    """
    from ruamel.yaml import YAML

    # ruamel's default round-trip loader only constructs plain mappings,
    # sequences and scalars (unknown tags raise) — unlike PyYAML's unsafe
    # ``yaml.load``, it cannot execute arbitrary Python.
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = 4096

    by_file: dict[Path, list[PlannedFix]] = {}
    for fix in planned:
        by_file.setdefault(fix.file, []).append(fix)

    applied: list[PlannedFix] = []
    skipped: list[SkippedFix] = []
    for file, fixes in by_file.items():
        with file.open() as f:
            doc = yaml.load(f)
        file_changed = False
        for fix in fixes:
            result = fix.mutate(doc)
            match result:
                case AutofixMutateResult.CHANGED:
                    applied.append(fix)
                    file_changed = True
                case AutofixMutateResult.NOOP:
                    # Already in the required state -- typically an earlier fix in
                    # this run edited the same entry (versioned models share one
                    # properties entry), or the artifacts are stale.
                    fix.description = f"{fix.description} (already present in file)"
                    applied.append(fix)
                case _:
                    skipped.append(
                        SkippedFix(
                            check_run_id=fix.check_run_id,
                            reason=f"no matching entry found in `{file}`",
                        )
                    )
        if file_changed and not dry_run:
            with file.open("w") as f:
                yaml.dump(doc, f)

    return applied, skipped
