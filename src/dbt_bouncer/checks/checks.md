# `check_column_data_must_end_underscore_date`

Columns with the type "DATE" must end with "_date".

**Example**:
```yaml
catalog_checks:
    - name: check_column_data_must_end_underscore_date
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
