`dbt-bouncer` runs checks against artifacts from dbt. Every check also has a unique [rule code](./rule_codes.md) (e.g. `MO021`) that can be used in place of its name. These checks fall into four categories:

- Catalog checks:
      - [Catalog Seeds](./catalog/check_catalog_seeds.md)
      - [Catalog Sources](./catalog/check_catalog_sources.md)
      - Columns
          - [Description](./catalog/columns/description.md)
          - [Naming](./catalog/columns/naming.md)
          - [Tests](./catalog/columns/tests.md)
- Information Schema checks (dbt 2.0+):
      - [Columns](./info_schema/columns.md)
      - [Grain](./info_schema/grain.md)
      - [Query](./info_schema/query.md)
      - [Sources](./info_schema/sources.md)
- Manifest checks:
      - [Exposures](./manifest/check_exposures.md)
      - [Lineage](./manifest/check_lineage.md)
      - [Macros](./manifest/check_macros.md)
      - [Metadata](./manifest/check_metadata.md)
      - Models
          - [Access](./manifest/models/access.md)
          - [Code](./manifest/models/code.md)
          - [Columns](./manifest/models/columns.md)
          - [Description](./manifest/models/description.md)
          - [Directories](./manifest/models/directories.md)
          - [Lineage](./manifest/models/lineage.md)
          - [Meta](./manifest/models/meta.md)
          - [Naming](./manifest/models/naming.md)
          - [Tags](./manifest/models/tags.md)
          - [Tests](./manifest/models/tests.md)
          - [Versioning](./manifest/models/versioning.md)
      - [Seeds](./manifest/check_seeds.md)
      - [Semantic Models](./manifest/check_semantic_models.md)
      - [Snapshots](./manifest/check_snapshots.md)
      - Sources
          - [Description](./manifest/sources/description.md)
          - [Freshness](./manifest/sources/freshness.md)
          - [Lineage](./manifest/sources/lineage.md)
          - [Loader](./manifest/sources/loader.md)
          - [Meta](./manifest/sources/meta.md)
          - [Naming](./manifest/sources/naming.md)
          - [Directories](./manifest/sources/directories.md)
          - [Tags](./manifest/sources/tags.md)
          - [Tests](./manifest/sources/tests.md)
      - [Tests](./manifest/check_tests.md)
      - [Unit Tests](./manifest/check_unit_tests.md)
- Run Results checks:
      - [Run Results](./run_results/check_run_results.md)

## Information Schema checks

dbt 2.0 and later can write the [dbt Information Schema](https://docs.getdbt.com/reference/info-schema): a set of Parquet tables that describe the project, including column-level lineage, inferred column types and the grain of each model. The JSON artifacts do not contain this data. `info_schema_checks` read it from `info_schema/v1/` in the dbt target directory, so `dbt_artifacts_dir` must point at a target directory that contains it.

To generate the Information Schema, add `--generate-info-schema` to a dbt command. Add `--static-analysis strict` to include column types and column-level lineage:

```shell
dbt build --static-analysis strict --generate-info-schema
```

dbt-bouncer reads `info_schema/v1/` and not the Parquet files under `target/private/`:

- dbt documents `info_schema/v1/` as a contracted interface with a versioned directory. dbt-bouncer refuses an Information Schema whose `schema_version` it does not support.
- `target/private/` is undocumented. It also holds only the data of the last dbt command, so a later command such as `dbt source freshness` replaces the lineage that `dbt build` wrote.

If `info_schema_checks` are configured and the directory does not exist, for example with dbt 1.x artifacts, `dbt-bouncer` exits with an artifact error.

Known limitations, from dbt:

- Column-level lineage needs `--static-analysis strict`. dbt skips a model whose SQL it cannot analyse, and records the lineage of an ephemeral model on the models that select from it.
- `dbt.node_columns.meta` is empty, so checks that use column `meta` read it from `manifest.json`.
- Inferred column types use Apache Arrow names, for example `Int64` ([dbt-labs/dbt#16515](https://github.com/dbt-labs/dbt/issues/16515)).
