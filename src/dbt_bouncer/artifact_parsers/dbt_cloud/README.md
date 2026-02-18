dbt Cloud now supports a "versionless" mode. Unfortunately `dbt-artifacts-parser` does not support any modifications to the schema od dbt artifacts using this mode, see [here](https://github.com/yu-iskw/dbt-artifacts-parser/pull/112#issuecomment-2360298424) for more info.

The purpose of this directory is to extend `dbt-artifacts-parser` to support dbt Cloud versionless mode by modifying the expected schema of dbt artifacts.

## Schema Source

The Pydantic models in `manifest_latest.py` were originally generated from the dbt JSON schema using `datamodel-code-generator`:

```bash
curl -s "https://schemas.getdbt.com/dbt/manifest/v12.json" > /tmp/manifest_v12.json
datamodel-codegen --input /tmp/manifest_v12.json --input-file-type jsonschema --output manifest_latest.py
```

## Manual Customizations

After generation, the following manual changes were made to match the official JSON schema:

### Base Class and Config

1. **Base class**: Changed from `BaseModel` to `BaseParserModel` (from `dbt_artifacts_parser.parsers.base`) to integrate with the dbt-artifacts-parser library.

2. **Config**: Changed from `class Config` to `model_config = ConfigDict(extra="allow")` for Pydantic v2 compatibility.

### Type Fixes

The JSON schema uses `anyOf` with `string`, `array<string>`, and `null` which `datamodel-code-generator` doesn't always handle correctly. The following fields have been manually fixed:

| Field | Original Generated Type | Correct Schema Type | Reason |
|-------|------------------------|---------------------|--------|
| `Owner.email` | `str \| None` | `str \| list[str] \| None` | dbt allows multiple owner emails |
| `Config.unique_key` | `str \| None` | `str \| list[str] \| None` | dbt allows multiple unique keys |
| `Config.tags` | `list[str] \| None` or `list[str]` | `str \| list[str] \| None` or `str \| list[str]` | dbt allows tags as string or list |

## Keeping in Sync

To regenerate the models from the latest schema:

```bash
# Install datamodel-code-generator
pip install datamodel-code-generator

# Download latest schema and regenerate
curl -s "https://schemas.getdbt.com/dbt/manifest/v12.json" > /tmp/manifest_v12.json
datamodel-codegen --input /tmp/manifest_v12.json --input-file-type jsonschema --output /tmp/generated.py

# Then manually:
# 1. Replace BaseModel with BaseParserModel
# 2. Replace class Config blocks with model_config = ConfigDict(extra="allow")
# 3. Fix any type issues (see Type Fixes section above)
```

Alternatively, compare types with `dbt-artifacts-parser` which tends to have more accurate types:
<https://github.com/yu-iskw/dbt-artifacts-parser/blob/main/dbt_artifacts_parser/parsers/manifest/manifest_v12.py>
