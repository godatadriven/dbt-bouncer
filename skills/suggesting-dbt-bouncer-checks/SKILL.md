---
name: suggesting-dbt-bouncer-checks
description: Analyzes a dbt project and suggests dbt-bouncer checks that already pass (for existing projects) or a sensible starter config (for greenfield projects). Use when adding dbt-bouncer to a project, creating or extending a dbt-bouncer.yml config, or enforcing dbt conventions with dbt-bouncer.
user-invocable: true
metadata:
  author: godatadriven
---

# Suggest dbt-bouncer checks for a dbt project

`dbt-bouncer` enforces conventions in dbt projects by running checks against dbt artifacts (`manifest.json`, `catalog.json`, `run_results.json`). This skill helps you propose a `dbt-bouncer.yml` config tailored to a project.

## Additional Resources

- [Check catalog](references/check-catalog.md) - Commonly suggested checks grouped by category
- [Config reference](references/config-reference.md) - Config file structure and global options

## Prerequisites

- `dbt-bouncer` installed (`pip install dbt-bouncer`).
- A `manifest.json` in the project's `target/` directory. If missing, run `dbt parse` (fast, does not hit the warehouse).
- `catalog.json` is only needed for `catalog_checks` (requires `dbt docs generate`). Prefer `manifest_checks` when the catalog is unavailable.

## Workflow: existing project (brownfield)

The golden rule: **suggested checks must pass against the current project**. Never suggest a config that breaks CI on day one.

1. **Inspect the project.** Read `dbt_project.yml`, the `models/` directory layout, and a sample of model `.yml` property files. Identify:
   - Layer structure (e.g. `staging/`, `intermediate/`, `marts/`) and naming prefixes (`stg_`, `int_`).
   - Documentation habits (are descriptions populated? which layers?).
   - Testing habits (`unique`/`not_null` on marts? relationship tests?).
   - Source conventions (freshness, loader, file naming).
2. **Codify what is already true.** Suggest checks that formalize existing conventions, e.g. if all staging models start with `stg_`, suggest `check_model_names` with `model_name_pattern: ^stg_` and `include: ^models/staging`.
3. **Scope aggressively.** Use `include`/`exclude` (regex on file path) to limit each check to where the convention actually holds.
4. **Use `severity: warn` for aspirational checks.** Checks the team *wants* but doesn't yet satisfy should be warnings, either per-check or globally at the top of the config.
5. **Validate before proposing.** Always run `dbt-bouncer run` against the drafted config and iterate until it exits 0 (warnings are acceptable, errors are not). Do not modify any model SQL or YAML to make a check pass — adjust the check's scope or severity instead.
6. **Present the config with rationale.** For each suggested check, state in one line which observed convention it codifies.

## Workflow: new project (greenfield)

No existing conventions to respect, so suggest an opinionated baseline:

1. Start from the low-controversy checks in [check-catalog.md](references/check-catalog.md): naming per layer, lineage rules between layers, `check_model_has_properties_file`, `check_model_description_populated`, `check_model_has_unique_test`, source freshness/loader checks.
2. Match the config to the project's planned layer structure (ask the user if unclear).
3. Set coverage checks (`check_model_documentation_coverage`, `check_model_test_coverage`) with achievable thresholds that can be ratcheted up later.
4. Recommend wiring `dbt-bouncer` into pre-commit and CI so conventions are enforced from the first model.

## Guardrails

- Never edit models, sources, seeds, or property files to satisfy a check unless the user explicitly asks.
- Only suggest check names that exist in the installed `dbt-bouncer` version. Verify with the documentation or by running the drafted config — unknown check names fail config validation.
- Prefer `manifest_checks` over `catalog_checks`/`run_results_checks` unless the corresponding artifacts are freshly generated.
- One convention per check entry; the same check name can appear multiple times with different `include` scopes.
