"""Registry meta-tests for the built-in check catalogue (U1).

Iterates the ``dbt_bouncer.checks`` entry points and verifies that every built-in
check satisfies the project's catalogue invariants:

1. Entry points cover every ``CheckCategory`` and discover the same check set as
   ``get_check_objects()`` (via ``pkgutil.walk_packages``).
2. Every check has a unique rule code registered in ``dbt_bouncer.enums.RuleCode``.
3. Every check appears in ``schema.json`` (both ``$defs`` and its category's
   discriminator mapping).
4. Every check has a documentation page under ``docs/checks/`` wired into
   ``mkdocs.yml`` and an entry in ``docs/checks/rule_codes.md``.
5. Every check has a ``Test<CheckClassName>`` test class in its mirror file under
   ``tests/unit/checks/`` exercising both passing and failing cases.
"""

from __future__ import annotations

import ast
import importlib
import json
import pkgutil
import re
import typing
from functools import lru_cache
from importlib.metadata import entry_points
from pathlib import Path
from typing import TYPE_CHECKING

from dbt_bouncer.cli.explain.utils import get_check_name
from dbt_bouncer.cli.list.utils import category_key
from dbt_bouncer.enums import CheckCategory, RuleCode
from dbt_bouncer.utils import _extract_checks_from_module, get_check_objects

if TYPE_CHECKING:
    from dbt_bouncer.check_framework.base import BaseCheck

_CHECKS_MODULE_PREFIX = "dbt_bouncer.checks."


@lru_cache(maxsize=1)
def _discover_entry_point_checks() -> tuple[type[BaseCheck], ...]:
    """Walk the ``dbt_bouncer.checks`` entry points and return discovered check classes.

    Returns:
        tuple[type[BaseCheck], ...]: Check classes discovered via entry points.

    """
    discovered: list[type[BaseCheck]] = []
    for ep in entry_points(group="dbt_bouncer.checks"):
        pkg = ep.load()
        for _, modname, _ in pkgutil.walk_packages(
            pkg.__path__, prefix=f"{pkg.__name__}."
        ):
            module = importlib.import_module(modname)
            _extract_checks_from_module(module, modname, discovered)

    seen: set[type[BaseCheck]] = set()
    unique: list[type[BaseCheck]] = []
    for cls in discovered:
        if cls not in seen:
            seen.add(cls)
            unique.append(cls)
    return tuple(sorted(unique, key=get_check_name))


def _mirror_unit_test_file(check_cls: type[BaseCheck]) -> Path:
    """Return the mirror unit test file path for a built-in check class.

    Args:
        check_cls: The built-in check class.

    Returns:
        Path: Expected path under ``tests/unit/checks/``.

    """
    subpath = check_cls.__module__.removeprefix(_CHECKS_MODULE_PREFIX).split(".")
    *dirs, stem = subpath
    test_stem = (
        f"test_{stem.removeprefix('check_')}"
        if stem.startswith("check_")
        else f"test_{stem}"
    )
    # Catalog top-level files keep `check_catalog_*` -> `test_catalog_*`, which
    # `removeprefix("check_")` already produces.
    return Path("tests/unit/checks", *dirs, f"{test_stem}.py")


def test_entry_points_discover_all_builtin_checks() -> None:
    """``dbt_bouncer.checks`` entry points cover every category and check class.

    Also guards against subpackages missing ``__init__.py``, which would cause
    ``pkgutil.walk_packages`` to silently skip their submodules.
    """
    eps = entry_points(group="dbt_bouncer.checks")
    assert {ep.name for ep in eps} == {c.directory for c in CheckCategory}

    ep_checks = _discover_entry_point_checks()
    builtin_checks = get_check_objects()

    assert len(ep_checks) >= 120, (
        f"docs/faq.md states dbt-bouncer ships 120+ built-in checks, found {len(ep_checks)}."
    )
    assert set(ep_checks) == set(builtin_checks), (
        "Entry-point walk and get_check_objects() discovered different check sets: "
        f"only_in_ep={set(ep_checks) - set(builtin_checks)}, "
        f"only_in_fs={set(builtin_checks) - set(ep_checks)}"
    )


def test_each_check_has_unique_rule_code_in_enum() -> None:
    """Every entry-point check has a unique rule code matching ``RuleCode``."""
    checks = _discover_entry_point_checks()
    pattern = re.compile(r"^[A-Z]{2}\d{3}$")

    codes_by_check: dict[str, str] = {}
    missing_or_invalid: list[str] = []
    for cls in checks:
        name = get_check_name(cls)
        code = getattr(cls, "code", None)
        if not isinstance(code, str) or not pattern.match(code):
            missing_or_invalid.append(f"{name}: {code!r}")
        else:
            codes_by_check[name] = code

    assert not missing_or_invalid, (
        f"Checks with missing or invalid rule codes: {missing_or_invalid}"
    )

    code_list = list(codes_by_check.values())
    duplicates = sorted({c for c in code_list if code_list.count(c) > 1})
    assert not duplicates, f"Duplicate rule codes assigned: {duplicates}"

    enum_codes = {
        str(member) for enum_cls in typing.get_args(RuleCode) for member in enum_cls
    }
    assigned_codes = set(code_list)
    assert assigned_codes == enum_codes, (
        "Mismatch between check rule codes and dbt_bouncer.enums.RuleCode: "
        f"unlisted_in_enum={sorted(assigned_codes - enum_codes)}, "
        f"unused_enum_codes={sorted(enum_codes - assigned_codes)}"
    )


def test_each_check_appears_in_schema_json() -> None:
    """Every entry-point check is present in ``schema.json`` definitions and mappings."""
    schema = json.loads(Path("schema.json").read_text(encoding="utf-8"))
    defs = schema.get("$defs", {})
    category_by_dir = {c.directory: c.value for c in CheckCategory}

    missing_defs: list[str] = []
    missing_discriminator: list[str] = []

    for cls in _discover_entry_point_checks():
        name = get_check_name(cls)
        if cls.__name__ not in defs:
            missing_defs.append(f"{name} ({cls.__name__})")

        cat_prop = category_by_dir[category_key(cls)]
        mapping = (
            schema.get("properties", {})
            .get(cat_prop, {})
            .get("items", {})
            .get("discriminator", {})
            .get("mapping", {})
        )
        expected_ref = f"#/$defs/{cls.__name__}"
        if mapping.get(name) != expected_ref:
            missing_discriminator.append(
                f"{name}: expected {expected_ref!r} in {cat_prop}, got {mapping.get(name)!r}"
            )

    assert not missing_defs, f"Checks missing from schema.json $defs: {missing_defs}"
    assert not missing_discriminator, (
        f"Checks missing from schema.json discriminator mappings: {missing_discriminator}"
    )


def test_each_check_has_docs_page() -> None:
    """Every entry-point check has a docs page in ``docs/checks/``, ``mkdocs.yml``, and ``rule_codes.md``."""
    mkdocs_text = Path("mkdocs.yml").read_text(encoding="utf-8")
    rule_codes_text = Path("docs/checks/rule_codes.md").read_text(encoding="utf-8")
    doc_file_cache: dict[Path, str] = {}

    errors: list[str] = []
    for cls in _discover_entry_point_checks():
        name = get_check_name(cls)
        code = getattr(cls, "code", None)
        module_subpath = cls.__module__.removeprefix(_CHECKS_MODULE_PREFIX)
        doc_rel = f"checks/{module_subpath.replace('.', '/')}.md"
        doc_path = Path("docs") / doc_rel

        if not doc_path.is_file():
            errors.append(f"{name}: missing docs file {doc_path}")
            continue

        if doc_path not in doc_file_cache:
            doc_file_cache[doc_path] = doc_path.read_text(encoding="utf-8")

        directive = f"::: {module_subpath}"
        if directive not in doc_file_cache[doc_path]:
            errors.append(f"{name}: {doc_path} is missing directive {directive!r}")

        if doc_rel not in mkdocs_text:
            errors.append(f"{name}: {doc_rel} is not listed in mkdocs.yml nav")

        if f"`{name}`" not in rule_codes_text or f"`{code}`" not in rule_codes_text:
            errors.append(f"{name} ({code}): missing from docs/checks/rule_codes.md")

    assert not errors, "Documentation coverage gaps:\n" + "\n".join(errors)


def test_each_check_has_passing_and_failing_unit_tests() -> None:
    """Every entry-point check has passing and failing unit tests in its mirror test file."""
    parsed_test_files: dict[Path, tuple[str, dict[str, ast.ClassDef]]] = {}
    errors: list[str] = []

    for cls in _discover_entry_point_checks():
        name = get_check_name(cls)
        expected_test_cls = f"Test{cls.__name__}"
        test_file = _mirror_unit_test_file(cls)

        if not test_file.is_file():
            errors.append(f"{name}: missing mirror test file {test_file}")
            continue

        if test_file not in parsed_test_files:
            source = test_file.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(test_file))
            classes = {
                node.name: node for node in tree.body if isinstance(node, ast.ClassDef)
            }
            parsed_test_files[test_file] = (source, classes)

        source, classes = parsed_test_files[test_file]
        cls_node = classes.get(expected_test_cls)
        if cls_node is None:
            errors.append(f"{name}: missing class {expected_test_cls} in {test_file}")
            continue

        cls_source = ast.get_source_segment(source, cls_node) or ""
        if f'"{name}"' not in cls_source and f"'{name}'" not in cls_source:
            errors.append(
                f"{name}: {expected_test_cls} in {test_file} does not reference {name!r}"
            )
        if "check_passes" not in cls_source:
            errors.append(
                f"{name}: {expected_test_cls} in {test_file} has no passing test (check_passes)"
            )
        if (
            "check_fails" not in cls_source
            and "DbtBouncerFailedCheckError" not in cls_source
        ):
            errors.append(
                f"{name}: {expected_test_cls} in {test_file} has no failing test (check_fails)"
            )

    assert not errors, "Unit test coverage gaps:\n" + "\n".join(errors)
