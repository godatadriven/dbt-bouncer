# dbt-bouncer

See [AGENTS.md](AGENTS.md) for shared project instructions.

## Claude-specific

- A pre-commit hook runs automatically via `.claude/settings.json` on Stop events.
- Use `/new-check` to scaffold a new check class with tests.
- Use `/build-artifacts` to regenerate test fixtures after dbt_project changes.
- In GitHub Actions workflows, always pin actions to full SHA commits with a version comment, e.g. `uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5 # v4`.
