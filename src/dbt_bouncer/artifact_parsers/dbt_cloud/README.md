dbt Cloud now supports a "versionless" mode. Unfortunately `dbt-artifacts-parser` does not support any modifications to the schema od dbt artifacts using this mode, see [here](https://github.com/yu-iskw/dbt-artifacts-parser/pull/112#issuecomment-2360298424) for more info.

The purpose of this directory is to extend `dbt-artifacts-parser` to support dbt Cloud versionless mode by modifying the expected schema of dbt artifacts.
