# `check_column_has_specified_test`
`

Columns that match the specified regexp pattern must have a specified test.

**Argument(s)**:

* `column_name_pattern`: Regex pattern to match the model name.
* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `test_name`: Name of the test to check for.

**Example**:
```yaml
catalog_checks:
    - name: check_column_has_specified_test
      column_name_pattern: ^is_.*
      test_name: not_null
```

**Required artifact(s)**:

* catalog.json
* manifest.json

---

# `check_column_name_complies_to_column_type`

Columns with specified data type must comply to the specified regexp naming pattern.

**Argument(s)**:

* `column_name_pattern`: Regex pattern to match the model name.
* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `types`: List of data types. These are specific to the dbt adapter you are using.

**Example**:
```yaml
catalog_checks:
    # DATE columns must end with "_date"
    - name: check_column_name_complies_to_column_type
      column_name_pattern: .*_date$
      types:
          - DATE
    # BOOLEAN columns must start with "is_"
    - name: check_column_name_complies_to_column_type
      column_name_pattern: ^is_.*
      types:
          - BOOLEAN
    # Columns of all types must consist of lowercase letters and underscores. Note that the specified types depend on the underlying database.
    - name: check_column_name_complies_to_column_type
      column_name_pattern: ^[a-z_]*$
      types:
          - BIGINT
          - BOOLEAN
          - DATE
          - DOUBLE
          - INTEGER
          - VARCHAR
```

**Required artifact(s)**:

* catalog.json
* manifest.json

---

# `check_columns_are_all_documented`
`

All columns in a model should be included in the model's properties file, i.e. `.yml` file.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
catalog_checks:
    - name: check_columns_are_all_documented
```

**Required artifact(s)**:

* catalog.json
* manifest.json

---

# `check_columns_are_documented_in_public_models`

Columns should have a populated description in public models.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
catalog_checks:
    - name: check_columns_are_documented_in_public_models
```

**Required artifact(s)**:

* catalog.json
* manifest.json

---

# `check_exposure_based_on_non_public_models`

Exposures should not be based on views.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the exposure path (i.e the `.yml` file where the exposure is configured). Only exposure paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_exposure_based_on_non_public_models
```

**Required artifact(s)**:

* manifest.json

---

# `check_exposure_based_on_view`

Exposures should not be based on views.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the exposure path (i.e the `.yml` file where the exposure is configured). Only exposure paths that match the pattern will be checked.
* `materializations_to_include`: (Optional) List of materializations to include in the check. If not provided, defaults to `ephemeral` and `view`.

**Example**:
```yaml
manifest_checks:
    - name: check_exposure_based_on_view
```

**Required artifact(s)**:

* manifest.json

---

# `check_lineage_permitted_upstream_models`

Upstream models must have a path that matches the provided `upstream_path_pattern`.

**Argument(s)**:

* `include`: Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `upstream_path_pattern`: Regexp pattern to match the upstream model(s) path.

**Example**:
```yaml
manifest_checks:
    - name: check_lineage_permitted_upstream_models
      include: ^staging
      upstream_path_pattern: $^
    - name: check_lineage_permitted_upstream_models
      include: ^intermediate
      upstream_path_pattern: ^staging|^intermediate
    - name: check_lineage_permitted_upstream_models
      include: ^marts
      upstream_path_pattern: ^staging|^intermediate
```

**Required artifact(s)**:

* manifest.json

---

# `check_lineage_seed_cannot_be_used`

Seed cannot be referenced in models with a path that matches the specified `include` config.

**Argument(s)**:

* `include`: Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_lineage_seed_cannot_be_used
      include: ^intermediate|^marts
```

**Required artifact(s)**:

* manifest.json

---

# `check_lineage_source_cannot_be_used`

Sources cannot be referenced in models with a path that matches the specified `include` config.

**Argument(s)**:

* `include`: Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_lineage_source_cannot_be_used
      include: ^intermediate|^marts
```

**Required artifact(s)**:

* manifest.json

---

# `check_macro_arguments_description_populated`

Macro arguments must have a populated description.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_macro_arguments_description_populated
```

**Required artifact(s)**:

* manifest.json

---

# `check_macro_code_does_not_contain_regexp_pattern`

The raw code for a macro must not match the specified regexp pattern.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
* `regexp_pattern`: The regexp pattern that should not be matched by the macro code.

**Example**:
```yaml
manifest_checks:
    # Prefer `coalesce` over `ifnull`: https://docs.sqlfluff.com/en/stable/rules.html#sqlfluff.rules.sphinx.Rule_CV02
    - name: check_macro_code_does_not_contain_regexp_pattern
      regexp_pattern: .*[i][f][n][u][l][l].*
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_code_does_not_contain_regexp_pattern`

The raw code for a model must not match the specified regexp pattern.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `regexp_pattern`: The regexp pattern that should not be matched by the model code.

**Example**:
```yaml
manifest_checks:
    # Prefer `coalesce` over `ifnull`: https://docs.sqlfluff.com/en/stable/rules.html#sqlfluff.rules.sphinx.Rule_CV02
    - name: check_model_code_does_not_contain_regexp_pattern
      regexp_pattern: .*[i][f][n][u][l][l].*
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_contract_enforced_for_public_model`

Public models must have contracts enforced.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_model_contract_enforced_for_public_model
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_depends_on_multiple_sources`

Models cannot reference more than one source.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_model_depends_on_multiple_sources
```

**Required artifacts(s)**:

* manifest.json

---

# `check_macro_description_populated`

Macros must have a populated description.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_macro_description_populated
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_directories`

Only specified sub-directories are permitted.

**Argument(s)**:

* `include`: Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
* `permitted_sub_directories`: List of permitted sub-directories.

**Example**:
```yaml
manifest_checks:
  # Special case for top level directories within `./models`, pass "" to `include`
  - name: check_model_directories
    include: ""
    permitted_sub_directories:
      - intermediate
      - marts
      - staging

  # Restrict sub-directories within `./models/staging`
  - name: check_model_directories
    include: ^staging
    permitted_sub_directories:
      - crm
      - payments
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_has_contracts_enforced`

Model must have contracts enforced.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
  - name: check_model_has_contracts_enforced
    include: ^marts
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_has_tags`

Models must have the specified tags.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `tags`: List of tags that must be present on the model.

**Example**:
```yaml
manifest_checks:
  - name: check_model_has_tags
    tags:
      - tag_1
      - tag_2
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_has_no_upstream_dependencies`

Identify if models have no upstream dependencies as this likely indicates hard-coded tables references.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
  - name: check_model_has_no_upstream_dependencies
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_max_chained_views`

Models cannot have more than the specified number of upstream dependents that are not tables (default: 3).

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `materializations_to_include`: (Optional) List of materializations to include in the check. If not provided, defaults to `ephemeral` and `view`.
* `max_chained_views`: (Optional) The maximum number of upstream dependents that are not tables. Default: 3

**Example**:
```yaml
manifest_checks:
  - name: check_model_max_chained_views
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_max_fanout`

Models cannot have more than the specified number of downstream models (default: 3).

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `max_downstream_models`: (Optional) The maximum number of permitted downstream models.

**Example**:
```yaml
manifest_checks:
  - name: check_model_max_fanout
    max_downstream_models: 2
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_max_upstream_dependencies`

Limit the number of upstream dependencies a model has. Default values are 5 for models, 5 for macros, and 1 for sources.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `max_upsream_macros`: (Optional) The maximum number of permitted upstream models. Default: 5.
* `max_upstream_models`: (Optional) The maximum number of permitted upstream macros. Default: 5
* `max_upstream_sources`: (Optional) The maximum number of permitted upstream sources. Default: 1

**Example**:
```yaml
manifest_checks:
  - name: check_model_max_upstream_dependencies
    max_upstream_models: 3
```

**Required artifacts(s)**:

* manifest.json

---

# `check_macro_name_matches_file_name`

Macros names must be the same as the file they are contained in.

Generic tests are also macros, however to document these tests the "name" value must be precededed with "test_".

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_macro_name_matches_file_name
```

**Required artifacts(s)**:

* manifest.json

---

# `check_macro_property_file_location`

Macro properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/5-the-rest-of-the-project#how-we-use-the-other-folders).

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_macro_property_file_location
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_access`

Models must have the specified access attribute. Requires dbt 1.7+.

**Argument(s)**:

* `access`: The access level to apply.
* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_model_access
      include: ^intermediate
      access: protected
    - name: check_model_access
      include: ^marts
      access: public
    - name: check_model_access
      include: ^staging
      access: protected
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_documentation_coverage`

Set the minimum percentage of models that have a populated description.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `min_model_documentation_coverage_pct`: The minimum percentage of models that must have a populated description. Default: 100

**Example**:
```yaml
manifest_checks:
    - name: check_model_documentation_coverage
      min_model_documentation_coverage_pct: 90
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_has_unique_test`

Models must have a test for uniqueness of a column.

**Argument(s)**:

* `accepted_uniqueness_tests`: (Optional) List of tests that are accepted as uniqueness tests. If not provided, defaults to `expect_compound_columns_to_be_unique`, `dbt_utils.unique_combination_of_columns` and `unique`.
* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_model_has_unique_test

    # Example of allowing a custom uniqueness test
    - name: check_model_has_unique_test
      accepted_uniqueness_tests:
        - expect_compound_columns_to_be_unique
        - my_custom_uniqueness_test
        - unique
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_description_populated`

Models must have a populated description.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_model_description_populated
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_documented_in_same_directory`

Models must be documented in the same directory where they are defined (i.e. `.yml` and `.sql` files are in the same directory).

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_model_documented_in_same_directory
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_has_meta_keys`

The `meta` config for models must have the specified keys.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `key`: A list (that may contain sub-lists) of required keys.

**Example**:
```yaml
manifest_checks:
    - name: check_model_has_meta_keys
      keys:
        - maturity
        - owner
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_names`

Models must have a name that matches the supplied regex.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `model_name_pattern`: Regex pattern to match the model name.

**Example**:
```yaml
manifest_checks:
    - name: check_model_names
      include: ^intermediate
      model_name_pattern: ^int_
    - name: check_model_names
      include: ^staging
      model_name_pattern: ^stg_
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_property_file_location`

Model properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_model_property_file_location
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_test_coverage`

Set the minimum percentage of models that have at least one test.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the model path. Only model paths that match the pattern will be checked.
* `min_model_test_coverage_pct`: The minimum percentage of models that must have at least one test. Default: 100

**Example**:
```yaml
manifest_checks:
    - name: check_model_test_coverage
      min_model_test_coverage_pct: 90
```

**Required artifacts(s)**:

* manifest.json

---

# `check_project_name`

Enforce that the name of the dbt project matches a supplied regex. Generally used to enforce that project names conform to something like  `company_<DOMAIN>`.

**Argument(s)**:

* `project_name_pattern`: Regex pattern to match the project name.

**Example**:
```yaml
manifest_checks:
    - name: check_project_name
```

**Required artifacts(s)**:

* manifest.json

---

# `check_run_results_max_gigabytes_billed`

Each result can have a maximum number of gigabytes billed. Note that this only works for the `dbt-bigquery` adapter.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the node path. Only node paths that match the pattern will be checked.
* `max_gigabytes_billed`: The maximum gigagbytes billed allowed for a node.

**Example**:
```yaml
run_results_checks:
    - name: check_run_results_max_gigabytes_billed
      max_gigabytes_billed: 100
```

**Required artifacts(s)**:

* manifest.json
* run_results.json

---

# `check_run_results_max_execution_time`

Each result can take a maximum duration (seconds).

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the node path. Only node paths that match the pattern will be checked.
* `max_execution_time`: The maximum execution time (seconds) allowed for a node.

**Example**:
```yaml
run_results_checks:
    - name: check_run_results_max_execution_time
      max_execution_time: 60
```

**Required artifacts(s)**:

* manifest.json
* run_results.json

---

# `check_source_columns_are_all_documented`

All columns in a source should be included in the source's properties file, i.e. `.yml` file.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the source path (i.e the `.yml` file where the source is configured). Only source paths that match the pattern will be checked.

**Example**:
```yaml
catalog_checks:
    - name: check_source_columns_are_all_documented
```

**Required artifacts(s)**:

* catalog.json
* manifest.json

---

# `check_source_description_populated`

Sources must have a populated description.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the source path (i.e the `.yml` file where the source is configured). Only source paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_source_description_populated
```

**Required artifacts(s)**:

* manifest.json

---

# `check_source_freshness_populated`

Sources must have a populated freshness.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the source path (i.e the `.yml` file where the source is configured). Only source paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_source_freshness_populated
```

**Required artifacts(s)**:

* manifest.json

---

# `check_source_loader_populated`

Sources must have a populated loader.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the source path (i.e the `.yml` file where the source is configured). Only source paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_source_loader_populated
```

**Required artifacts(s)**:

* manifest.json

---

# `check_source_has_meta_keys`

The `meta` config for sources must have the specified keys.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the source path (i.e the `.yml` file where the source is configured). Only source paths that match the pattern will be checked.
* `key`: A list (that may contain sub-lists) of required keys.

**Example**:
```yaml
manifest_checks:
    - name: check_source_has_meta_keys
      keys:
        - contact:
            - email
            - slack
        - owner
```

**Required artifacts(s)**:

* manifest.json

---

# `check_source_has_tags`

Sources must have the specified tags.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the source path (i.e the `.yml` file where the source is configured). Only source paths that match the pattern will be checked.
* `tags`: List of tags that must be present on the model.

**Example**:
```yaml
manifest_checks:
    - name: check_source_has_tags
     tags:
      - tag_1
      - tag_2
```

**Required artifacts(s)**:

* manifest.json

---

# `check_source_names`

Sources must have a name that matches the supplied regex.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the source path (i.e the `.yml` file where the source is configured). Only source paths that match the pattern will be checked.
* `source_name_pattern`: Regex pattern to match the source name.

**Example**:
```yaml
manifest_checks:
    - name: check_source_names
      source_name_pattern: >
        ^[a-z0-9_]*$
```

**Required artifacts(s)**:

* manifest.json

---

# `check_source_property_file_location`

Source properties files must follow the guidance provided by dbt [here](https://docs.getdbt.com/best-practices/how-we-structure/1-guide-overview).

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the source path (i.e the `.yml` file where the source is configured). Only source paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_source_property_file_location
```

**Required artifacts(s)**:

* manifest.json

---

# `check_source_not_orphaned`

Sources must be referenced in at least one model.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the source path (i.e the `.yml` file where the source is configured). Only source paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_source_not_orphaned
```

**Required artifacts(s)**:

* manifest.json

---

# `check_source_used_by_models_in_same_directory`

Sources can only be referenced by models that are located in the same directory where the source is defined.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the source path (i.e the `.yml` file where the source is configured). Only source paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_source_used_by_models_in_same_directory
```

**Required artifacts(s)**:

* manifest.json

---

# `check_source_used_by_only_one_model`

Each source can be references by a maximum of one model.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the source path (i.e the `.yml` file where the source is configured). Only source paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_source_used_by_only_one_model
```

**Required artifacts(s)**:

* manifest.json
