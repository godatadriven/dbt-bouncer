# CLI

This page provides documentation for the `dbt-bouncer` CLI.

::: mkdocs-click
    :command: cli
    :module: dbt_bouncer.main
    :prog_name: dbt-bouncer
    :show_hidden: False
    :style: plain

## Run command

The `run` subcommand executes dbt-bouncer checks against your dbt project:

```bash
dbt-bouncer run --config-file dbt-bouncer.yml
```

This is the primary command for running checks. For backwards compatibility, `dbt-bouncer` (without the `run` subcommand) still works and behaves identically.

All the main CLI options (`--check`, `--only`, `--output-file`, etc.) work with both `dbt-bouncer run` and the legacy `dbt-bouncer` invocation.

## Validate command

The `validate` subcommand checks your configuration file for common issues:

```bash
dbt-bouncer validate --config-file dbt-bouncer.yml
```

It will report:

- YAML syntax errors with line numbers
- Missing required fields (like `name` in checks)
- Incorrect configuration types (e.g., if a check category is not a list)

Example output for a valid config:

```text
Config file is valid!
```

Example output for issues:

```text
Found 2 issue(s) in config file:
  Line 1: Check is missing required 'name' field
  Line 3: YAML syntax error: ...
```

## CLI Options

The following options are available for the `run` command (and the legacy `dbt-bouncer` invocation):

### `--config-file`

**Type:** Path
**Default:** `dbt-bouncer.yml`
**Required:** No

Specifies the location of the YAML configuration file containing your dbt-bouncer checks.

**Example:**

```bash
dbt-bouncer run --config-file config/checks.yml
```

### `--check`

**Type:** String (comma-separated)
**Default:** Empty (runs all checks)
**Required:** No

Limits the checks run to specific check names. Multiple checks can be specified as a comma-separated list.

**Examples:**

```bash
# Run a single check
dbt-bouncer run --check check_model_has_unique_test

# Run multiple checks
dbt-bouncer run --check check_model_names,check_source_freshness_populated
```

### `--only`

**Type:** String (comma-separated)
**Default:** Empty (runs all categories)
**Required:** No

Limits the checks run to specific categories. Multiple categories can be specified as a comma-separated list.

**Examples:**

```bash
# Run only manifest checks
dbt-bouncer run --only manifest_checks

# Run catalog and manifest checks
dbt-bouncer run --only catalog_checks,manifest_checks
```

### `--output-file`

**Type:** Path
**Default:** None (outputs to stdout)
**Required:** No

Specifies the location where check metadata will be saved. If not provided, results are written to stdout.

**Example:**

```bash
dbt-bouncer run --output-file results/check-results.json
```

### `--output-format`

**Type:** Choice
**Options:** `csv`, `json`, `junit`, `sarif`, `tap`
**Default:** `json`
**Required:** No

Specifies the format for the output file or stdout when no output file is specified.

**Examples:**

```bash
# Output as JSON (default)
dbt-bouncer run --output-format json

# Output as JUnit XML for CI integration
dbt-bouncer run --output-format junit --output-file results.xml

# Output as SARIF for GitHub Code Scanning
dbt-bouncer run --output-format sarif --output-file results.sarif
```

### `--output-only-failures`

**Type:** Flag
**Default:** False
**Required:** No

When passed, only failures will be included in the output file. Successful checks are omitted.

**Example:**

```bash
dbt-bouncer run --output-file results.json --output-only-failures
```

### `--show-all-failures`

**Type:** Flag
**Default:** False
**Required:** No

When passed, all failures will be printed to the console, even if an output file is specified.

**Example:**

```bash
dbt-bouncer run --show-all-failures
```

### `-v, --verbosity`

**Type:** Counter
**Default:** 0
**Required:** No

Controls the verbosity of logging output. Can be specified multiple times to increase verbosity.

**Examples:**

```bash
# Basic logging
dbt-bouncer run -v

# More verbose logging
dbt-bouncer run -vv

# Maximum verbosity
dbt-bouncer run -vvv
```

## Exit codes

`dbt-bouncer` returns the following exit codes:

- `0`: All checks have succeeded.

- `1`:

    - At least one check has failed. Check the logs for more information.
    - A fatal error occurred during check setup or check execution. Check the logs for more information.
