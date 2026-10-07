# Information Schema Checks: Grain

!!! note

    The below checks require `manifest.json` and the dbt Information Schema (`info_schema/v1/` in the dbt target directory) to be present. dbt 2.0 and later write the Information Schema when a command runs with `--generate-info-schema`. Add `--static-analysis strict` to include column types and column-level lineage. See [Information Schema checks](../index.md#information-schema-checks) for details.

::: info_schema.grain
