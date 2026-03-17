"""Generate a JSON Schema from the dbt-bouncer Pydantic config model.

The schema is written to ``schema.json`` at the repository root. It reflects
all built-in checks and their parameters, enabling editor autocomplete and
validation when editing ``dbt-bouncer.yml`` config files.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from pydantic.json_schema import GenerateJsonSchema

from dbt_bouncer.logger import configure_console_logging

SCHEMA_PATH = Path("schema.json")

# Fields injected at runtime by the runner — not user-configurable. These use
# IsInstanceSchema types that Pydantic cannot convert to JSON Schema, so we
# strip them from the output entirely.
_RUNTIME_FIELDS = frozenset(
    {
        "catalog_node",
        "catalog_source",
        "exposure",
        "index",
        "macro",
        "model",
        "run_result",
        "seed",
        "semantic_model",
        "snapshot",
        "source",
        "test",
        "unit_test",
    }
)


class _PermissiveSchemaGenerator(GenerateJsonSchema):
    """Custom generator that tolerates IsInstanceSchema types.

    Check classes contain resource fields (e.g. ``model``, ``source``) typed as
    artifact wrapper objects. Pydantic raises on these by default because they
    use ``IsInstanceSchema`` core schemas. We return an empty object so schema
    generation succeeds, then strip these fields in post-processing.
    """

    def is_instance_schema(self, schema: Any) -> dict[str, Any]:  # noqa: ARG002
        return {}


def _strip_runtime_fields(schema: dict[str, Any]) -> None:
    """Remove runtime-injected resource fields from ``$defs`` entries."""
    for definition in schema.get("$defs", {}).values():
        props = definition.get("properties", {})
        required = definition.get("required", [])
        for field_name in _RUNTIME_FIELDS:
            props.pop(field_name, None)
        definition["required"] = [r for r in required if r not in _RUNTIME_FIELDS]
        if not definition["required"]:
            definition.pop("required", None)


def _sort_schema(schema: dict[str, Any]) -> dict[str, Any]:
    """Sort schema for deterministic output across environments.

    Check discovery order varies between Python versions and platforms, so we
    sort ``$defs``, ``oneOf`` arrays, and discriminator mappings alphabetically.

    Returns:
        dict[str, Any]: The sorted schema.

    """
    # Sort $defs by key.
    if "$defs" in schema:
        schema["$defs"] = dict(sorted(schema["$defs"].items()))

    # Sort oneOf arrays and discriminator mappings inside properties.
    for prop in schema.get("properties", {}).values():
        items = prop.get("items", {})
        if "oneOf" in items:
            items["oneOf"] = sorted(items["oneOf"], key=lambda x: x.get("$ref", ""))
        disc = items.get("discriminator", {})
        if "mapping" in disc:
            disc["mapping"] = dict(sorted(disc["mapping"].items()))

    return schema


def main() -> None:
    """Build the dynamic DbtBouncerConf model and export its JSON Schema."""
    configure_console_logging(0)

    # Import all check modules so the full discriminated union is built.
    import dbt_bouncer.checks.catalog  # noqa: F401
    import dbt_bouncer.checks.manifest  # noqa: F401
    import dbt_bouncer.checks.run_results  # noqa: F401
    from dbt_bouncer.checks.common import NestedDict
    from dbt_bouncer.config_file_parser import create_bouncer_conf_class

    logging.info("Building DbtBouncerConf model with all checks...")
    DbtBouncerConf = create_bouncer_conf_class()  # noqa: N806
    DbtBouncerConf.model_rebuild(_types_namespace={"NestedDict": NestedDict})

    logging.info("Generating JSON Schema...")
    schema = DbtBouncerConf.model_json_schema(
        mode="validation",
        schema_generator=_PermissiveSchemaGenerator,
    )

    # Strip runtime resource fields that are not user-configurable.
    _strip_runtime_fields(schema)

    # Sort for deterministic output across environments.
    schema = _sort_schema(schema)

    # Add top-level schema metadata.
    schema["$schema"] = "https://json-schema.org/draft/2020-12/schema"
    schema["$id"] = (
        "https://raw.githubusercontent.com/godatadriven/dbt-bouncer/main/schema.json"
    )

    SCHEMA_PATH.write_text(json.dumps(schema, indent=2) + "\n")
    logging.info("Schema written to %s", SCHEMA_PATH)


if __name__ == "__main__":
    main()
