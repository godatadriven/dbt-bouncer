# dbt-bouncer

See [AGENTS.md](AGENTS.md) for shared project instructions.

## Claude-specific

- A pre-commit hook runs automatically via `.claude/settings.json` on Stop events.
- Use `/new-check` to scaffold a new check class with tests.
- Use `/build-artifacts` to regenerate test fixtures after dbt_project changes.
