import logging
import os
import re
import tomllib
import types
import typing
from collections.abc import Iterable, Mapping
from functools import lru_cache
from pathlib import Path, PurePath
from typing import TYPE_CHECKING, Any

import jellyfish
import typer
import yaml
from pydantic import RootModel, ValidationError

from dbt_bouncer.enums import CheckCategory, ConfigFileName, ConfigFileSource
from dbt_bouncer.exceptions import DbtBouncerConfigError
from dbt_bouncer.utils import compile_pattern, get_check_registry, load_config_from_yaml

if TYPE_CHECKING:
    from dbt_bouncer.check_framework.base import BaseCheck
    from dbt_bouncer.configuration_file.parser import DbtBouncerConfBase

_rebuilt_classes: set[str] = set()

_CHECK_CATEGORIES = tuple(CheckCategory)

# Rule codes are a two-letter prefix plus three digits (e.g. "MO001"); check
# names are snake_case, so the shape alone distinguishes them.
RULE_CODE_PATTERN = re.compile(r"^[A-Z]{2}\d{3}$")

# Deprecated check names accepted for backwards compatibility. Rewritten to
# their replacement (with a warning) before config validation, and removed in
# the next major release.
DEPRECATED_CHECK_NAME_ALIASES: dict[str, str] = {
    "check_model_description_contains_regex_pattern": "check_model_description_contains_regexp_pattern",
}


def _suggest_closest(target: str, candidates: Iterable[str]) -> str:
    """Return a did-you-mean sentence for the candidate closest to ``target``.

    Returns:
        str: ``Did you mean '<candidate>'?`` when the closest candidate is
        within a Levenshtein distance of 3, else an empty string. The cap
        avoids surfacing absurd suggestions for keys that resemble nothing.
        The caller is responsible for surrounding punctuation and spacing.

    """
    best = min(
        candidates,
        key=lambda c: jellyfish.levenshtein_distance(c, target),
        default=None,
    )
    if best is not None:
        distance = jellyfish.levenshtein_distance(best, target)
        if distance <= 3:
            return f"Did you mean '{best}'?"
    return ""


def warn_deprecated_check_name(old_name: str, new_name: str) -> None:
    """Log the standard deprecation warning for a renamed check.

    Args:
        old_name: The deprecated check name found in the user's input.
        new_name: The replacement check name.

    """
    logging.warning(
        f"Check name `{old_name}` is deprecated and will be removed in a future major release; use `{new_name}` instead."
    )


def apply_deprecated_check_name_aliases(config_file_contents: dict) -> dict:
    """Rewrite deprecated check names to their replacements, warning per use.

    Returns:
        dict: The same mapping, mutated in place.

    """
    for category, checks in config_file_contents.items():
        if not category.endswith("_checks") or not isinstance(checks, list):
            continue
        for c in checks:
            if not isinstance(c, dict):
                continue
            new_name = DEPRECATED_CHECK_NAME_ALIASES.get(c.get("name"))
            if new_name is not None:
                warn_deprecated_check_name(c["name"], new_name)
                c["name"] = new_name
    return config_file_contents


@lru_cache(maxsize=1)
def _base_field_names() -> tuple[str, ...]:
    """Return the names of the cacheable scalar fields on ``DbtBouncerConfBase``.

    Derived from ``DbtBouncerConfBase.model_fields`` minus the three check
    categories (which are dynamic and handled separately). Computing this on
    demand means any new base field is picked up by the cache automatically —
    no hand-maintained tuple to drift out of sync. Cached for the lifetime of
    the interpreter (the base model doesn't change at runtime), mirroring the
    pattern used by ``_internal_checks_digest`` in ``utils.py``.

    Returns:
        tuple[str, ...]: Sorted field names to persist in the cache payload.

    """
    from dbt_bouncer.configuration_file.parser import DbtBouncerConfBase

    return tuple(
        sorted(
            name
            for name in DbtBouncerConfBase.model_fields
            if name not in _CHECK_CATEGORIES
        )
    )


DEFAULT_DBT_BOUNCER_CONFIG = """manifest_checks:
  - name: check_model_directories
    include: ^models
    permitted_sub_directories:
      - intermediate
      - marts
      - staging
  - name: check_model_names
    include: ^models/staging
    model_name_pattern: ^stg_
"""


def get_config_file_path(
    config_file: PurePath,
    config_file_source: ConfigFileSource,
) -> PurePath:
    """Get the path to the config file for dbt-bouncer. This is fetched from (in order):

    1. The file passed via the `--config-file` CLI flag.
    2. The file passed via the `DBT_BOUNCER_CONFIG_FILE` environment variable.
    3. A file named `dbt-bouncer.yml` in the current working directory.
    4. A file named `dbt-bouncer.yaml` in the current working directory.
    5. A file named `dbt-bouncer.toml` in the current working directory.
    6. A `[tool.dbt-bouncer]` section in `pyproject.toml` (in current working directory or parent directories).

    Returns:
        PurePath: Config file for dbt-bouncer.

    Raises:
        DbtBouncerConfigError: If no config file is found.

    """  # ruff: ignore[missing-trailing-period, missing-terminal-punctuation]
    logging.debug(f"{config_file=}")
    logging.debug(f"{config_file_source=}")

    if config_file_source == ConfigFileSource.COMMANDLINE:
        logging.debug(f"Config file passed via command line: {config_file}")
        config_file_path = Path(config_file)
        if not config_file_path.exists():
            raise DbtBouncerConfigError(f"Config file not found: {config_file}")
        return config_file

    if config_file_path_via_env_var := os.getenv("DBT_BOUNCER_CONFIG_FILE"):
        logging.debug(
            f"Config file passed via environment variable: {config_file_path_via_env_var}"
        )
        return Path(config_file_path_via_env_var)

    if config_file_source == ConfigFileSource.DEFAULT:
        logging.debug(f"Using default value for config file: {config_file}")
        config_file_path = Path.cwd() / config_file
        if config_file_path.exists():
            return config_file_path

    # Check for dbt-bouncer.yaml in the current working directory. The `.yml`
    # default is preferred (checked above), so `.yaml` acts as a fallback.
    yaml_config_path = Path.cwd() / ConfigFileName.DBT_BOUNCER_YAML
    if yaml_config_path.exists():
        logging.debug(f"Found dbt-bouncer.yaml: {yaml_config_path}")
        return yaml_config_path

    # Check for dbt-bouncer.toml in the current working directory
    toml_config_path = Path.cwd() / ConfigFileName.DBT_BOUNCER_TOML
    if toml_config_path.exists():
        logging.debug(f"Found dbt-bouncer.toml: {toml_config_path}")
        return toml_config_path

    # Read config from pyproject.toml
    logging.debug("Loading config from pyproject.toml, if exists...")
    if (Path().cwd() / ConfigFileName.PYPROJECT_TOML).exists():
        pyproject_toml_dir = Path().cwd()
    else:
        pyproject_toml_dir = next(
            (
                parent
                for parent in Path().cwd().parents
                if (parent / ConfigFileName.PYPROJECT_TOML).exists()
            ),
            None,
        )  # i.e. look in parent directories for a pyproject.toml file

        if pyproject_toml_dir is None:
            logging.debug("No pyproject.toml found.")
            raise DbtBouncerConfigError(
                "No config file found. Please provide a `dbt-bouncer.yml`, `dbt-bouncer.yaml`, `dbt-bouncer.toml`, or a `pyproject.toml` with a `[tool.dbt-bouncer]` section. Alternatively, pass the path via the `--config-file` flag.",
            )

    return pyproject_toml_dir / ConfigFileName.PYPROJECT_TOML


def load_config_file_contents(
    config_file_path: PurePath,
    allow_default_config_file_creation: bool | None = None,
) -> Mapping[str, Any]:
    """Load the contents of the config file.

    Args:
        config_file_path: Path to the config file.
        allow_default_config_file_creation: Whether to allow the creation of a default config file if one does not exist. Used to allow pytesting of this function.

    Returns:
        Mapping[str, Any]: Config for dbt-bouncer.

    Raises:
        DbtBouncerConfigError: If the config file type is not supported or does not contain the expected keys.

    """
    match config_file_path.suffix:
        case ".yml" | ".yaml":
            return load_config_from_yaml(Path(config_file_path))
        case ".toml":
            with Path(config_file_path).open("rb") as f:
                toml_cfg = tomllib.load(f)

            # dbt-bouncer.toml: config is at the top level
            if config_file_path.name == ConfigFileName.DBT_BOUNCER_TOML:
                return toml_cfg

            # pyproject.toml: config is under [tool.dbt-bouncer]
            if toml_cfg.get("tool", {}).get("dbt-bouncer"):
                return toml_cfg["tool"]["dbt-bouncer"]
            else:
                logging.warning(
                    "Cannot find a `dbt-bouncer.yml` file, `dbt-bouncer.toml` file, or a `[tool.dbt-bouncer]` section in pyproject.toml."
                )
                if (
                    allow_default_config_file_creation is True
                    and os.getenv("CREATE_DBT_BOUNCER_CONFIG_FILE") != "false"
                    and (
                        os.getenv("CREATE_DBT_BOUNCER_CONFIG_FILE") == "true"
                        or typer.confirm(
                            "Do you want `dbt-bouncer` to create a `dbt-bouncer.yml` file in the current directory?"
                        )
                    )
                ):
                    created_config_file = Path.cwd().joinpath(
                        ConfigFileName.DBT_BOUNCER_YML
                    )
                    created_config_file.touch()
                    logging.info(
                        "A `dbt-bouncer.yml` file has been created in the current directory with default settings."
                    )
                    with Path.open(created_config_file, "w") as f:
                        f.write(DEFAULT_DBT_BOUNCER_CONFIG)

                    return load_config_from_yaml(created_config_file)

                else:
                    raise DbtBouncerConfigError(
                        "No configuration for `dbt-bouncer` could be found. You can pass the path to your config file via the `--config-file` flag. Alternatively, configure `pyproject.toml` or use a `dbt-bouncer.toml` file.",
                    )
        case _:
            raise DbtBouncerConfigError(
                f"Config file must be a `.toml`, `.yaml`, or `.yml` file. Got {config_file_path.suffix}."
            )


def lint_config_file(config_file_path: Path) -> list[dict[str, Any]]:
    """Lint the config file and return a list of issues with line numbers.

    Args:
        config_file_path: Path to the config file.

    Returns:
        list[dict[str, Any]]: List of issues found, each with 'line', 'message', and 'severity'.

    """
    issues: list[dict[str, Any]] = []

    if config_file_path.suffix not in [".yml", ".yaml"]:
        return issues

    try:
        content = config_file_path.read_text()
        data = yaml.load(content, Loader=yaml.CSafeLoader)  # type: ignore[possibly-missing-attribute]
    except yaml.YAMLError as e:
        problem_mark = getattr(e, "problem_mark", None)
        if problem_mark:
            issues.append(
                {
                    "line": problem_mark.line + 1,
                    "message": f"YAML syntax error: {getattr(e, 'problem', str(e))}",
                    "severity": "error",
                }
            )
        else:
            issues.append(
                {
                    "line": 1,
                    "message": f"YAML syntax error: {e}",
                    "severity": "error",
                }
            )
        return issues
    except Exception as e:
        logging.warning(f"Unexpected error during config parsing: {e}")
        issues.append(
            {
                "line": 1,
                "message": f"Unexpected error during config parsing: {e}",
                "severity": "error",
            }
        )
        return issues

    if not data:
        issues.append(
            {
                "line": 1,
                "message": "Config file is empty",
                "severity": "error",
            }
        )
        return issues

    registry = get_check_registry()
    valid_categories = set(CheckCategory)
    for category in valid_categories:
        if category in data:
            checks = data[category]
            if not isinstance(checks, list):
                issues.append(
                    {
                        "line": 1,
                        "message": f"'{category}' must be a list, got {type(checks).__name__}",
                        "severity": "error",
                    }
                )
                continue

            for idx, check in enumerate(checks):
                if not isinstance(check, dict):
                    issues.append(
                        {
                            "line": idx + 1,
                            "message": f"Check must be a dictionary, got {type(check).__name__}",
                            "severity": "error",
                        }
                    )
                    continue

                # Treat absent, null and empty values alike: all leave us with
                # nothing to look up in the registry.
                raw_name = check.get("name") or check.get("code")
                if not raw_name:
                    issues.append(
                        {
                            "line": idx + 1,
                            "message": "Check is missing required 'name' or 'code' field",
                            "severity": "error",
                        }
                    )
                    continue  # Cannot validate if absent

                # The config is arbitrary user YAML, so a name may be any type.
                # Report the type plainly instead of stringifying it and then
                # offering a nearest-match suggestion for something that was
                # never a name -- a list value would otherwise produce
                # "Unknown check name '['check_model_names']'".
                if not isinstance(raw_name, str):
                    issues.append(
                        {
                            "line": idx + 1,
                            "message": (
                                f"Check 'name' must be a string, got "
                                f"{type(raw_name).__name__}"
                            ),
                            "severity": "error",
                        }
                    )
                    continue

                check_name = raw_name

                if check_name in DEPRECATED_CHECK_NAME_ALIASES:
                    warn_deprecated_check_name(
                        check_name, DEPRECATED_CHECK_NAME_ALIASES[check_name]
                    )
                elif check_name not in registry:
                    best_match = min(
                        registry.keys(),
                        key=lambda k, target=check_name: jellyfish.levenshtein_distance(
                            k, target
                        ),
                        default=None,
                    )
                    suggestion = f" Did you mean '{best_match}'?" if best_match else ""
                    issues.append(
                        {
                            "line": idx + 1,
                            "message": f"Unknown check name '{check_name}'.{suggestion}",
                            "severity": "error",
                        }
                    )

    return issues


def _resolve_loc_line(config_file_path: Path, loc: tuple[Any, ...]) -> int:
    """Best-effort line number for a Pydantic error location in a YAML file.

    Walks the YAML node tree (which carries source marks) along ``loc``.
    Discriminated-union tags (check names) appear in ``loc`` but are not YAML
    keys, so unresolvable string parts are skipped and the walk continues.

    Returns:
        int: 1-based line number of the deepest resolvable part, or 1 when the
        file is not YAML or nothing resolves.

    """
    if config_file_path.suffix not in (".yml", ".yaml"):
        return 1

    try:
        node = yaml.compose(  # type: ignore[possibly-missing-attribute]
            config_file_path.read_text(),
            Loader=yaml.CSafeLoader,  # type: ignore[possibly-missing-attribute]
        )
    except (OSError, yaml.YAMLError):
        return 1

    line = 1
    for part in loc:
        match node:
            case yaml.MappingNode():
                for key_node, value_node in node.value:
                    if key_node.value == str(part):
                        line = key_node.start_mark.line + 1
                        node = value_node
                        break
                # An unmatched part (e.g. a union tag) is skipped; the walk
                # continues from the current node.
            case yaml.SequenceNode() if isinstance(part, int):
                if part >= len(node.value):
                    break
                node = node.value[part]
                line = node.start_mark.line + 1
            case _:
                break
    return line


def lint_config_file_deep(config_file_path: Path) -> list[dict[str, Any]]:
    """Validate the config file against the full Pydantic model.

    Complements ``lint_config_file``: the surface lint catches YAML syntax and
    shape issues, this catches everything ``dbt-bouncer run`` would reject —
    unknown keys, unknown check parameters, and mistyped parameter values.

    Args:
        config_file_path: Path to the config file.

    Returns:
        list[dict[str, Any]]: Issues found, each with 'line', 'message', and
        'severity'.

    """
    issues: list[dict[str, Any]] = []

    try:
        config_file_contents = dict(
            load_config_file_contents(
                config_file_path, allow_default_config_file_creation=False
            )
        )
    except DbtBouncerConfigError as e:
        return [{"line": 1, "message": str(e), "severity": "error"}]

    check_categories = [
        k
        for k in config_file_contents
        if k.endswith("_checks") and config_file_contents.get(k) != []
    ]

    custom_checks_dir = None
    if config_file_contents.get("custom_checks_dir"):
        custom_checks_dir = (
            Path(config_file_path).parent / config_file_contents["custom_checks_dir"]
        )

    try:
        validate_conf(
            check_categories=check_categories,
            config_file_contents=config_file_contents,
            custom_checks_dir=custom_checks_dir,
        )
    except DbtBouncerConfigError as e:
        details = e.details or [{"loc": (), "message": m} for m in str(e).splitlines()]
        for detail in details:
            issues.append(
                {
                    "line": _resolve_loc_line(
                        config_file_path, tuple(detail.get("loc", ()))
                    ),
                    "message": detail["message"],
                    "severity": "error",
                }
            )
    except Exception as e:
        logging.warning(f"Unexpected error during config validation: {e}")
        issues.append(
            {
                "line": 1,
                "message": f"Unexpected error during config validation: {e}",
                "severity": "error",
            }
        )

    return issues


def _get_stub_namespace() -> dict[str, Any]:
    """Return a lightweight namespace for Pydantic model_rebuild() during config validation.

    ``NestedDict`` is the only forward reference used in check field annotations
    that must resolve to a real class. All artifact wrapper types now use ``Any``
    directly in ``BaseCheck``, so no stubs are needed for them.

    Returns:
        dict[str, Any]: Namespace mapping type names for ``model_rebuild()``.

    """
    from dbt_bouncer.check_framework.exceptions import NestedDict

    return {"NestedDict": NestedDict}


_CONF_CACHE_FORMAT_VERSION = 1

_root_model_fields_cache: dict[type, tuple[tuple[str, type[RootModel]], ...]] = {}


def _find_root_model_in_annotation(annotation: Any) -> type[RootModel] | None:
    """Walk ``annotation`` and return the first ``RootModel`` subclass found.

    Handles plain types, ``X | None`` (PEP 604 union) and ``Optional[X]`` /
    ``Union[X, ...]``. Used to identify check fields whose JSON-mode dump must
    be re-coerced into a typed Pydantic instance after ``model_construct``.

    Returns:
        type[RootModel] | None: The RootModel subclass, or ``None`` if the
        annotation does not contain one.

    """
    if isinstance(annotation, type) and issubclass(annotation, RootModel):
        return annotation

    if typing.get_origin(annotation) in (typing.Union, types.UnionType):
        for arg in typing.get_args(annotation):
            found = _find_root_model_in_annotation(arg)
            if found is not None:
                return found

    return None


def _root_model_fields(
    cls: type["BaseCheck"],
) -> tuple[tuple[str, type[RootModel]], ...]:
    """Return the ``(field_name, RootModel_subclass)`` pairs for ``cls``.

    Cached per class — model_fields are immutable after class creation, so the
    lookup only runs once even when the cache contains many entries of the
    same check type.

    Returns:
        tuple[tuple[str, type[RootModel]], ...]: Pairs to re-coerce after
        ``cls.model_construct``.

    """
    cached = _root_model_fields_cache.get(cls)
    if cached is not None:
        return cached

    pairs: list[tuple[str, type[RootModel]]] = []
    for name, field in cls.model_fields.items():
        rm = _find_root_model_in_annotation(field.annotation)
        if rm is not None:
            pairs.append((name, rm))
    result = tuple(pairs)
    _root_model_fields_cache[cls] = result
    return result


def _construct_cached_check(
    cls: type["BaseCheck"], data: dict[str, Any]
) -> "BaseCheck":
    """Rebuild a cached check instance without triggering Pydantic schema generation.

    ``cls.model_validate`` would lazily build the discriminated-union schema for
    the check class (~2-3ms per class, 200ms+ total on a real config). We can
    skip that work because the data was already validated on the cold path —
    the cache only stores primitive field values extracted from a valid
    instance by ``_dump_check_for_cache``.

    The one wrinkle is that ``model_construct`` does not coerce primitives
    back into ``RootModel`` instances. Check fields typed as ``NestedDict``
    are JSON-dumped as raw lists/dicts/strings, so we re-wrap them manually
    before construction. ``StrEnum`` fields (``severity``, ``materialization``)
    compare equal to their raw strings, so they need no coercion.

    Returns:
        BaseCheck: The reconstructed check instance.

    """
    rm_fields = _root_model_fields(cls)
    if not rm_fields:
        return cls.model_construct(**data)

    # Shallow-copy before mutating: the caller's dict (``item["data"]`` in the
    # cache payload) is not ours to modify in place.
    data = {**data}
    for fname, rm_cls in rm_fields:
        value = data.get(fname)
        if value is not None and not isinstance(value, rm_cls):
            data[fname] = rm_cls.model_validate(value)

    return cls.model_construct(**data)


@lru_cache(maxsize=1)
def _get_lite_conf_class() -> type["DbtBouncerConfBase"]:
    """Return a lightweight Pydantic subclass of ``DbtBouncerConfBase``.

    The warm cache path skips the expensive discriminated-union model build by
    rehydrating already-validated check instances into a class whose category
    fields are typed as ``list[Any]``. Constructing this class is a sub-millisecond
    operation versus ~70ms for the full discriminated-union variant.

    Cached for the interpreter's lifetime since the class is immutable.

    Returns:
        type[DbtBouncerConfBase]: A subclass with three ``list[Any]`` category fields.

    """
    from pydantic import Field, create_model

    from dbt_bouncer.configuration_file.parser import DbtBouncerConfBase

    cls = create_model(
        "DbtBouncerConfLite",
        __base__=DbtBouncerConfBase,
        catalog_checks=(list[Any], Field(default=[])),
        manifest_checks=(list[Any], Field(default=[])),
        run_results_checks=(list[Any], Field(default=[])),
    )
    return cls


def _conf_cache_enabled() -> bool:
    """Whether the validated-conf disk cache is active.

    Disabled when the ``DBT_BOUNCER_DISABLE_CONF_CACHE`` env var is set to a
    truthy value. Useful for tests that monkeypatch check classes or environment
    state in ways the cache key cannot capture.

    Returns:
        bool: ``True`` if caching should run.

    """
    return os.environ.get("DBT_BOUNCER_DISABLE_CONF_CACHE", "").lower() not in (
        "1",
        "true",
        "yes",
    )


def _load_cached_conf(
    cache_path: Path,
    configured_check_names: set[str],
    custom_checks_dir: Path | None,
) -> "DbtBouncerConfBase | None":
    """Try to load a cached, validated bouncer-config from disk.

    Builds a strict ``(module, qualname) -> class`` lookup from the classes
    that ``get_check_objects_for_names`` actually loaded for the configured
    check names. The cache payload is then resolved against this map only —
    nothing outside that vouched-for set is reachable, so a corrupted cache
    file cannot pull in arbitrary modules.

    Cached check instances are rebuilt via ``cls.model_construct`` to skip
    the lazy Pydantic schema generation that ``model_validate`` triggers
    (~2-3ms per class, dominating the warm-load cost on real configs). The
    only fields that need explicit re-coercion are ``RootModel`` subclasses
    (e.g. ``NestedDict``) which JSON-dump to primitives; those are wrapped
    back into typed instances by ``_construct_cached_check`` before
    construction. ``StrEnum`` fields compare equal to their raw string forms,
    so they need no coercion.

    Returns:
        The cached config, or ``None`` if the cache is missing, corrupt, or
        references a class that wasn't part of the loaded set.

    """
    if not cache_path.exists():
        return None

    import orjson

    try:
        raw = cache_path.read_bytes()
    except OSError:
        return None

    try:
        payload = orjson.loads(raw)
    except orjson.JSONDecodeError:
        logging.debug("Conf cache unreadable, rebuilding.", exc_info=True)
        return None

    if payload.get("v") != _CONF_CACHE_FORMAT_VERSION:
        return None

    # Load only the check classes referenced by the user's configured names
    # and build a strict allow-list keyed by (module, qualname). The cache
    # payload can only reference classes in this map.
    class_map: dict[tuple[str, str], type[BaseCheck]] = {}
    if configured_check_names:
        from dbt_bouncer.utils import get_check_objects_for_names

        for cls in get_check_objects_for_names(
            frozenset(configured_check_names),
            custom_checks_dir=custom_checks_dir,
        ):
            class_map[cls.__module__, cls.__qualname__] = cls

    base_fields = payload.get("base", {})
    checks_by_cat = payload.get("checks", {})

    materialised: dict[str, Any] = dict(base_fields)
    for cat, items in checks_by_cat.items():
        out: list[Any] = []
        for item in items:
            cls = class_map.get((item["_module"], item["_qualname"]))
            if cls is None:
                logging.debug(
                    "Conf cache references unresolved check class %s.%s; rebuilding.",
                    item["_module"],
                    item["_qualname"],
                )
                return None
            out.append(_construct_cached_check(cls, item["data"]))
        materialised[cat] = out

    return _get_lite_conf_class().model_construct(**materialised)


def _dump_check_for_cache(check: "BaseCheck") -> dict[str, Any]:
    """Extract a check instance's field values without invoking ``model_dump``.

    Calling ``model_dump`` on the check itself would lazily build the Pydantic
    serialiser schema for each check class (~1.7ms per class, ~160ms total on
    a real config) — wasted work given every field value is already a
    JSON-compatible primitive validated on the cold path. ``RootModel`` fields
    (e.g. the recursive ``NestedDict``) still go through ``model_dump``, but
    those classes are shared across checks so their serialiser is built once;
    ``_construct_cached_check`` re-wraps them on load. Any value orjson cannot
    serialise surfaces as a ``TypeError`` in the caller, which skips the cache
    write gracefully.

    Returns:
        dict[str, Any]: Field name → primitive value mapping.

    """
    data: dict[str, Any] = {}
    for field_name in type(check).model_fields:
        value = getattr(check, field_name)
        if isinstance(value, RootModel):
            value = value.model_dump(mode="json")
        data[field_name] = value
    return data


def _write_cached_conf(cache_path: Path, bouncer_config: "DbtBouncerConfBase") -> None:
    """Serialise the relevant fields of ``bouncer_config`` to ``cache_path``.

    The dynamic ``DbtBouncerConf`` subclass produced by ``create_model`` can't
    survive a process restart, so this writes a JSON payload containing the
    base field values and, per check, its source module + class qualname plus
    the dict produced by ``_dump_check_for_cache()``. Reload happens via
    ``cls.model_construct(**data)`` which restores the typed instance without
    re-running validation.
    """
    import orjson

    base_fields = {
        name: getattr(bouncer_config, name, None) for name in _base_field_names()
    }

    checks_by_cat: dict[str, list[dict[str, Any]]] = {}
    for cat in _CHECK_CATEGORIES:
        items: list[dict[str, Any]] = []
        for check in getattr(bouncer_config, cat, []) or []:
            cls = type(check)
            items.append(
                {
                    "_module": cls.__module__,
                    "_qualname": cls.__qualname__,
                    "data": _dump_check_for_cache(check),
                }
            )
        checks_by_cat[str(cat)] = items

    payload = {
        "v": _CONF_CACHE_FORMAT_VERSION,
        "base": base_fields,
        "checks": checks_by_cat,
    }

    try:
        blob = orjson.dumps(payload)
    except (TypeError, orjson.JSONEncodeError):
        logging.debug("Conf cache write failed during serialisation.", exc_info=True)
        return

    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = cache_path.with_suffix(cache_path.suffix + ".tmp")
        tmp.write_bytes(blob)
        tmp.replace(cache_path)
    except OSError:
        logging.debug("Conf cache write failed.", exc_info=True)


def validate_conf(
    check_categories,  #: list[Literal["catalog_checks"], Literal["manifest_checks"], Literal["run_results_checks"]],
    config_file_contents: dict[str, Any],
    custom_checks_dir: Path | None = None,
) -> "DbtBouncerConfBase":
    """Validate the configuration and return the Pydantic model.

    Returns:
        DbtBouncerConf: The validated configuration.

    Raises:
        DbtBouncerConfigError: If the configuration is invalid.

    """
    logging.info("Validating conf...")

    config_file_contents = apply_deprecated_check_name_aliases(config_file_contents)

    # Normalize check entries and extract check names/codes from config to enable
    # targeted module loading. Resolving a rule code to its check name needs the
    # full registry, which imports every check module — the very cost targeted
    # loading exists to avoid — so only build it when a code is actually used.
    registry: dict[str, type[BaseCheck]] | None = None
    configured_check_names: set[str] = set()
    for cat in check_categories:
        for entry in config_file_contents.get(cat, []):
            if not isinstance(entry, dict):
                continue
            # A rule code may appear under either key, so match on its shape.
            c_key = entry.get("name") or entry.get("code")
            if not c_key:
                continue
            configured_check_names.add(c_key)
            if not isinstance(c_key, str) or not RULE_CODE_PATTERN.match(c_key):
                # A plain check name needs no resolution, and the check's own
                # `code` field default fills the code in during validation.
                continue
            if registry is None:
                registry = get_check_registry(custom_checks_dir)
            cls = registry.get(c_key)
            if cls is None:
                continue
            name_field = cls.model_fields.get("name")
            if name_field is not None:
                args = typing.get_args(name_field.annotation)
                if args:
                    entry["name"] = args[0]
                    configured_check_names.add(args[0])
            code_val = getattr(cls, "code", None)
            if code_val is not None:
                entry["code"] = code_val
                configured_check_names.add(code_val)

    cache_path: Path | None = None
    if _conf_cache_enabled():
        from dbt_bouncer.utils import compute_conf_cache_key, get_cache_dir
        from dbt_bouncer.version import version

        ver = version()
        cache_key = compute_conf_cache_key(
            ver,
            config_file_contents,
            list(check_categories),
            custom_checks_dir=custom_checks_dir,
        )
        cache_path = get_cache_dir() / f"conf_{ver}_{cache_key}.json"
        cached = _load_cached_conf(
            cache_path, configured_check_names, custom_checks_dir
        )
        if cached is not None:
            logging.debug("Loaded validated conf from cache: %s", cache_path)
            return cached

    if configured_check_names:
        # Fast path: import only modules containing the configured checks.
        from dbt_bouncer.configuration_file.parser import _create_conf_class
        from dbt_bouncer.utils import get_check_objects_for_names

        check_objects = get_check_objects_for_names(
            frozenset(configured_check_names),
            custom_checks_dir=custom_checks_dir,
        )
        DbtBouncerConf = _create_conf_class(  # ruff: ignore[non-lowercase-variable-in-function]
            custom_checks_dir=custom_checks_dir,
            check_categories=frozenset(check_categories),
            check_objects=check_objects,
        )
    else:
        # Fallback: no check names to extract, use full scan.
        if CheckCategory.CATALOG_CHECKS in check_categories:
            import dbt_bouncer.checks.catalog
        if CheckCategory.MANIFEST_CHECKS in check_categories:
            import dbt_bouncer.checks.manifest
        if CheckCategory.RUN_RESULTS_CHECKS in check_categories:
            import dbt_bouncer.checks.run_results  # ruff: ignore[unused-import]

        from dbt_bouncer.configuration_file.parser import create_bouncer_conf_class

        DbtBouncerConf = create_bouncer_conf_class(  # ruff: ignore[non-lowercase-variable-in-function]
            custom_checks_dir=custom_checks_dir,
            check_categories=frozenset(check_categories),
        )
    class_key = f"{DbtBouncerConf.__module__}.{DbtBouncerConf.__qualname__}"
    if class_key not in _rebuilt_classes:
        DbtBouncerConf.model_rebuild(_types_namespace=_get_stub_namespace())
        _rebuilt_classes.add(class_key)

    try:
        bouncer_config = DbtBouncerConf(**config_file_contents)
    except ValidationError as e:
        check_registry = get_check_registry(custom_checks_dir)
        accepted_names = list(check_registry.keys())
        details: list[dict[str, Any]] = []
        for error in e.errors():
            loc = error["loc"]
            location = " -> ".join(str(part) for part in loc)
            if (
                compile_pattern(
                    r"Input tag \S* found using 'name' does not match any of the expected tags: [\S\s]*",
                    flags=re.DOTALL,
                ).match(error["msg"])
                is not None
            ):
                incorrect_name = error["msg"][
                    error["msg"].find("tag") + 5 : error["msg"].find("found using") - 2
                ]
                # Unlike ``_suggest_closest``, no distance cap is applied here:
                # a check entry must name a registered check, so the nearest
                # registry entry is always the most useful pointer, even for a
                # badly mangled name.
                min_name = min(
                    accepted_names,
                    key=lambda name, target=incorrect_name: (
                        jellyfish.levenshtein_distance(name, target)
                    ),
                    default=None,
                )
                suggestion = f" Did you mean '{min_name}'?" if min_name else ""
                message = f"Check '{incorrect_name}' does not match any of the expected checks.{suggestion}"
            elif error["type"] == "extra_forbidden":
                # An unknown key was found. Suggest the closest valid key: for
                # a top-level key the candidates come from the conf class, for
                # a check-level key from the check class named by the union
                # tag in ``loc`` (e.g. ("manifest_checks", 0, "check_x", key)).
                extra_key = str(loc[-1])
                candidates: set[str] = set()
                if len(loc) == 1:
                    candidates = set(DbtBouncerConf.model_fields)
                elif len(loc) >= 4:
                    check_cls = check_registry.get(str(loc[2]))
                    if check_cls is not None:
                        resource_field = getattr(check_cls, "iterate_over", None)
                        candidates = {
                            f
                            for f in check_cls.model_fields
                            if f not in ("index", resource_field)
                        }
                suggestion = _suggest_closest(extra_key, candidates)
                message = f"{location}: {error['msg']}"
                if suggestion:
                    message = f"{message}. {suggestion}"
            else:
                message = f"{location}: {error['msg']}"
            details.append({"loc": loc, "message": message})

        raise DbtBouncerConfigError(
            "\n".join(f"{i + 1}. {d['message']}" for i, d in enumerate(details)),
            details=details,
        ) from e

    if cache_path is not None:
        _write_cached_conf(cache_path, bouncer_config)

    return bouncer_config
