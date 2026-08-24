# CLI

This page provides documentation for the `dbt-bouncer` CLI.

## run

The `run` subcommand executes dbt-bouncer checks against your dbt project:

```bash
dbt-bouncer run --config-file dbt-bouncer.yml
```

This is the primary command for running checks. For backwards compatibility, `dbt-bouncer` (without the `run` subcommand) still works and behaves identically.

All the main CLI options (`--check`, `--only`, `--output-file`, etc.) work with both `dbt-bouncer run` and the legacy `dbt-bouncer` invocation. The `--dry-run` option is only available via `dbt-bouncer run --dry-run`.

### Options

#### `--config-file`

**Type:** Path
**Default:** `dbt-bouncer.yml`
**Required:** No

Specifies the location of the YAML configuration file containing your dbt-bouncer checks.

**Example:**

```bash
dbt-bouncer run --config-file config/checks.yml
```

#### `--dry-run`

**Type:** Flag
**Default:** False
**Required:** No

When passed, assembles the full check list as normal but prints a summary table showing the check name, resource type, and count for each check that would run — then exits with code 0 without executing any checks. Useful for previewing which checks are in scope before a full run.

**Example:**

```bash
dbt-bouncer run --dry-run
```

Example output:

```text
╭─ Dry run — checks that would execute ─────────────────────╮
│ Check name                     │ Resource type │ Count     │
│ CheckModelNamePattern          │ model         │  1234     │
│ CheckModelDescriptionPopulated │ model         │  1234     │
│ CheckSourceDescriptionPopulated│ source        │    56     │
╰────────────────────────────────────────────────────────────╯

Dry run complete. 2524 check(s) would run.
```

#### `--check`

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

#### `--only`

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

#### `--output-file`

**Type:** Path
**Default:** None (no output file is written)
**Required:** No

Specifies the location where check metadata will be saved. If not provided, no structured output file is written.

**Example:**

```bash
dbt-bouncer run --output-file results/check-results.json
```

#### `--output-format`

**Type:** Choice
**Options:** `csv`, `json`, `junit`, `sarif`, `tap`
**Default:** `json`
**Required:** No

Specifies the format for the output file. Requires `--output-file` to be set.

**Examples:**

```bash
# Output as JSON (default)
dbt-bouncer run --output-format json

# Output as JUnit XML for CI integration
dbt-bouncer run --output-format junit --output-file results.xml

# Output as SARIF for GitHub Code Scanning
dbt-bouncer run --output-format sarif --output-file results.sarif
```

#### `--output-only-failures`

**Type:** Flag
**Default:** False
**Required:** No

When passed, only failures will be included in the output file. Successful checks are omitted.

**Example:**

```bash
dbt-bouncer run --output-file results.json --output-only-failures
```

#### `--show-all-failures`

**Type:** Flag
**Default:** False
**Required:** No

When passed, all failures will be printed to the console, even if an output file is specified.

**Example:**

```bash
dbt-bouncer run --show-all-failures
```

#### `-v, --verbosity`

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

## validate

The `validate` subcommand checks your configuration file for common issues:

```bash
dbt-bouncer validate --config-file dbt-bouncer.yml
```

It will report:

- YAML syntax errors with line numbers
- Missing required fields (like `name` in checks)
- Incorrect configuration types (e.g., if a check category is not a list)
- Everything `dbt-bouncer run` would reject: unknown keys, unknown check parameters, and mistyped parameter values — with a "Did you mean" suggestion for the closest valid name

Example output for a valid config:

```text
Config file is valid!
```

Example output for issues:

```text
Found 2 issue(s) in config file:
  Line 1: Check is missing required 'name' field
  Line 3: model_name_patern: Extra inputs are not permitted. Did you mean 'model_name_pattern'?
```

### Options

#### `--config-file`

**Type:** Path
**Default:** `dbt-bouncer.yml`
**Required:** No

Specifies the location of the YAML configuration file to validate.

**Example:**

```bash
dbt-bouncer validate --config-file config/checks.yml
```

## init

The `init` subcommand creates a `dbt-bouncer.yml` configuration file interactively:

```bash
dbt-bouncer init
```

It asks a series of questions and writes a starter configuration file based on your answers:

- Where your dbt artifacts are located (default: `target`)
- Whether to check that all models have descriptions
- Whether to check that all models have a unique test
- Whether to enforce naming conventions for staging models

If a `dbt-bouncer.yml` file already exists, you will be prompted before it is overwritten.

## list

The `list` subcommand lists all available dbt-bouncer checks, grouped by category:

```bash
dbt-bouncer list
```

### Options

#### `--output-format`

**Type:** Choice
**Options:** `json`, `text`
**Default:** `text`
**Required:** No

Controls the output format. Use `json` for machine-readable output.

**Examples:**

```bash
# List checks as human-readable text (default)
dbt-bouncer list

# List checks as JSON
dbt-bouncer list --output-format json
```

## explain

The `explain` subcommand describes one check: what it does, its configurable parameters, and example usage. It works offline and does not need dbt artifacts:

```bash
dbt-bouncer explain check_model_names
```

A check can be referenced by its name or by its rule code, so `dbt-bouncer explain MO038` shows the same output. An unknown name or code exits with code 2 (`CONFIG_ERROR`) and prints the closest match.

Example output:

```text
╭─ check_model_names (MO038) ─────────────────────────────────────╮
│ Models must have a name that matches the supplied regex.        │
│ ...                                                             │
╰────────────────────────────────────────────────────── manifest ─╯
Parameters
╭─────────────────────┬──────┬──────────┬─────────╮
│ Name                │ Type │ Required │ Default │
├─────────────────────┼──────┼──────────┼─────────┤
│ model_name_pattern  │ str  │   yes    │ -       │
╰─────────────────────┴──────┴──────────┴─────────╯
Documentation: https://godatadriven.github.io/dbt-bouncer/checks/manifest/models/naming/
```

### Options

#### `--custom-checks-dir`

**Type:** Path
**Default:** None
**Required:** No

Directory containing custom checks. When passed, custom checks can be explained too.

#### `--output-format`

**Type:** Choice
**Options:** `json`, `text`
**Default:** `text`
**Required:** No

Controls the output format. Use `json` for machine-readable output.

**Example:**

```bash
dbt-bouncer explain check_model_names --output-format json
```

## mcp

The `mcp` subcommand starts a [Model Context Protocol](https://modelcontextprotocol.io) server on the stdio transport:

```bash
dbt-bouncer mcp
```

This lets AI coding agents (Claude Code, Cursor, etc.) query and run dbt-bouncer. The server exposes four tools:

- `read_project_config`: Read and validate the project's config file. The agent learns which conventions the project enforces before it generates dbt code.
- `list_checks`: List all available checks, grouped by category.
- `explain_check`: Explain one check by name or rule code: description, parameters, and example usage.
- `run_checks`: Run the configured checks against the project's dbt artifacts. This requires at minimum a `manifest.json` (from `dbt parse`).

The command requires the optional `mcp` dependency:

```bash
pip install 'dbt-bouncer[mcp]'
```

Example client configuration (Claude Code, `.mcp.json`):

```json
{
  "mcpServers": {
    "dbt-bouncer": {
      "command": "dbt-bouncer",
      "args": ["mcp"]
    }
  }
}
```

## Exit codes

`dbt-bouncer` returns distinct exit codes so that CI pipelines and scripts can tell a check failure apart from a setup problem:

- `0` (`SUCCESS`): All checks have succeeded.
- `1` (`CHECK_ERRORS`): At least one check has failed. Check the logs for more information.
- `2` (`CONFIG_ERROR`): The config file is missing, unreadable, or invalid (e.g. an invalid `--only` value).
- `3` (`ARTIFACT_ERROR`): A required dbt artifact (`manifest.json`, `catalog.json`, `run_results.json`) is missing or was generated with an unsupported dbt version.
- `4` (`NO_CHECKS_RUN`): No checks ran because the config matched no resources. Check the `package_name`, the config file, the dbt artifacts, and any `--check` or `--only` filters.

These codes apply to both `dbt-bouncer run` and `dbt-bouncer validate` (which only ever returns `SUCCESS`, `CHECK_ERRORS`, or `CONFIG_ERROR` — for `validate`, `CHECK_ERRORS` means lint issues were found in the config file, not that dbt checks failed).
