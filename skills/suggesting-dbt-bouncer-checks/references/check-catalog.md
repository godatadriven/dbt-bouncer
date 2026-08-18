# Commonly suggested checks

Not exhaustive — see the [dbt-bouncer docs](https://godatadriven.github.io/dbt-bouncer/) for the full list. Grouped by how safe they are to suggest.

## Low-controversy baseline (good for greenfield)

```yaml
manifest_checks:
  # Naming per layer
  - name: check_model_names
    include: ^models/staging
    model_name_pattern: ^stg_
  - name: check_model_names
    include: ^models/intermediate
    model_name_pattern: ^int_

  # Layered lineage
  - name: check_lineage_permitted_upstream_models
    include: ^models/staging
    upstream_path_pattern: $^          # staging models only select from sources
  - name: check_lineage_permitted_upstream_models
    include: ^models/marts
    upstream_path_pattern: ^models/staging|^models/intermediate
  - name: check_model_does_not_directly_join_to_source
  - name: check_lineage_source_cannot_be_used
    include: ^models/intermediate|^models/marts

  # Documentation & properties
  - name: check_model_has_properties_file
  - name: check_model_description_populated
  - name: check_model_documented_in_same_directory

  # Testing
  - name: check_model_has_unique_test
  - name: check_model_incremental_has_unique_key

  # Sources
  - name: check_source_description_populated
  - name: check_source_freshness_populated
  - name: check_source_loader_populated

  # SQL hygiene
  - name: check_model_hard_coded_references
  - name: check_model_has_semi_colon
```

## Convention-dependent (verify against the project first)

- `check_model_directories` — permitted subdirectories per layer
- `check_model_materialization_permitted` — e.g. staging must be `view`/`ephemeral`
- `check_model_access` / `check_model_contract_enforced_for_public_model` — governance
- `check_model_has_meta_keys` / `check_source_has_meta_keys` — e.g. `owner`
- `check_model_schema_name`, `check_model_file_name`, `check_source_file_name`
- `check_model_does_not_use_select_star`, `check_model_max_number_of_lines`

## Coverage / ratchet checks (start with `severity: warn` or a low threshold)

- `check_model_documentation_coverage` (`min_model_documentation_coverage_pct`)
- `check_model_test_coverage`
- `check_unit_test_coverage`
- `check_model_max_fanout`, `check_model_max_chained_views`

## Require catalog.json (`catalog_checks`)

- `check_columns_are_all_documented`
- `check_column_name_complies_to_column_type` / `check_column_type_complies_to_column_name`
- `check_column_has_specified_test`

## Require run_results.json (`run_results_checks`)

- `check_run_results_max_execution_time`
- `check_run_results_max_gigabytes_billed` (BigQuery only)
