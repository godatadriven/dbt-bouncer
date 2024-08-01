# `check_column_name_complies_to_column_type`

Columns with specified data type must comply to the specified regexp naming pattern.

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

# `check_model_description_populated`

Models must have a populated description.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.

**Example**:
```yaml
manifest_checks:
    - name: check_model_description_populated
```

**Required artifacts(s)**:

* manifest.json

---

# `check_model_names`

Models must have a name that matches the supplied regex.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
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

# `check_project_name`

Enforce that the name of the dbt project matches a supplied regex. Generally used to enforce that project names conform to something like  `company_<DOMAIN>`.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
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

* `max_gigabytes_billed`: The maximum execution time (seconds) allowed for a node.

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

# `check_source_has_meta_keys`

The `meta` config for sources must have the specified keys.

**Argument(s)**:

* `include`: (Optional) Regex pattern to match the macro path. Only macro paths that match the pattern will be checked.
* `key`:

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

# `check_top_level_directories`

Only specified top-level directories are allowed to contain models.

**Example**:
```yaml
manifest_checks:
    - name: check_top_level_directories
```

**Required artifacts(s)**:

* manifest.json
