## How to run `dbt-bouncer`

1. Generate dbt artifacts by running a dbt command:

    * `dbt parse` to generate a `manifest.json` artifact (no database connection required!).
    * `dbt docs generate` to generate a `catalog.json` artifact (necessary if you are using catalog checks).
    * `dbt run` (or any other command that implies it e.g. `dbt build`) to generate a `run_results.json` artifact (necessary if you are using run results checks).

1. Create a config file (`dbt-bouncer.yml`, `dbt-bouncer.toml`, or a `[tool.dbt-bouncer]` section in `pyproject.toml`), details [here](./config_file.md). Alternatively, you can run `dbt-bouncer init` to generate a basic configuration file.

1. Run `dbt-bouncer` to validate that your conventions are being maintained.

---

### Installing with Python

Install from [pypi.org](https://pypi.org/p/dbt-bouncer):

```shell
pip install dbt-bouncer # or via any other package manager
```

Run:

```shell
dbt-bouncer --config-file <PATH_TO_CONFIG_FILE>
```

```shell
Running dbt-bouncer (X.X.X)...
Loaded config from dbt-bouncer-example.yml...
Validating conf...
```

`dbt-bouncer` also supports a verbose mode, run:

```shell
dbt-bouncer --config-file <PATH_TO_CONFIG_FILE> -v
```

```shell
INFO: Running dbt-bouncer (0.0.0)...
DEBUG: config_file=PosixPath('dbt-bouncer-example.yml')
DEBUG: config_file_source='COMMANDLINE'
DEBUG: Config file passed via command line: dbt-bouncer-example.yml
DEBUG: Loading config from /home/user/dbt-bouncer/dbt-bouncer-example.yml...
INFO: Loaded config from dbt-bouncer-example.yml...
```

When parsing artifacts, `dbt-bouncer` displays a summary table of discovered resources:

```shell
            Parsed artifacts for
            'dbt_bouncer_test_project'
╭──────────────────┬─────────────────┬───────╮
│ Artifact         │ Category        │ Count │
├──────────────────┼─────────────────┼───────┤
│ manifest.json    │ Exposures       │     2 │
│                  │ Macros          │     3 │
│                  │ Nodes           │    12 │
│                  │ Seeds           │     3 │
│                  │ Semantic Models │     1 │
│                  │ Snapshots       │     2 │
│                  │ Sources         │     4 │
│                  │ Tests           │    36 │
│                  │ Unit Tests      │     3 │
│ catalog.json     │ Nodes           │    13 │
│                  │ Sources         │     0 │
│ run_results.json │ Results         │    51 │
╰──────────────────┴─────────────────┴───────╯
```

!!! tip "Trade-offs"
    **Best for:** manual runs, quick one-off validation, local development, scripting.

    **Watch out:** manual — easy to forget before committing or opening a PR.

---

### Running as an executable using [uv](https://github.com/astral-sh/uv)

Run `dbt-bouncer` as a standalone Python executable using `uv`:

```shell
uvx dbt-bouncer --config-file <PATH_TO_CONFIG_FILE>
```

!!! tip "Trade-offs"
    **Best for:** environments or machines without a persistent Python install.

    **Watch out:** slightly slower than a local install due to ephemeral environment creation.

---

### Pre-commit hooks / prek

`dbt-bouncer` ships an official [pre-commit](https://pre-commit.com/) hook that runs automatically before each commit. Add it to your `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/godatadriven/dbt-bouncer
    rev: vX.X.X # Check https://github.com/godatadriven/dbt-bouncer/releases for the latest version
    hooks:
      - id: dbt-bouncer
        args: ["--config-file", "<PATH_TO_CONFIG_FILE>"] # Optional
```

Alternatively, use a local hook (requires `dbt-bouncer` to be available in your environment):

```yaml
- repo: local
  hooks:
    - id: dbt-bouncer
      name: dbt-bouncer
      entry: dbt-bouncer # --config-file <PATH_TO_CONFIG_FILE>
      language: system
      pass_filenames: false
      always_run: true
```

For full setup details see the [FAQ](./faq.md#how-to-set-up-dbt-bouncer-with-prekpre-commit).

!!! tip "Trade-offs"
    **Best for:** catching violations immediately during development, before code is pushed to remote.

    **Watch out:** dbt artifacts must already exist — you need to run `dbt parse` (or another dbt command), possibly in the prior hook, before this hook can execute. This adds latency to every commit. The hook can also be bypassed with `git commit --no-verify`.

---

### GitHub Actions

Run `dbt-bouncer` as part of your CI pipeline:

```yaml
name: CI pipeline

on:
  pull_request:
      branches:
          - main

jobs:
    run-dbt-bouncer:
        permissions:
            pull-requests: write # Required to write a comment on the PR
        runs-on: ubuntu-latest
        steps:
            - name: Checkout
              uses: actions/checkout@v4

            - name: Generate or fetch dbt artifacts
              run: ...

            - uses: godatadriven/dbt-bouncer@vX.X
              with:
                check: '' # optional, comma-separated check names to run
                config-file: ./<PATH_TO_CONFIG_FILE>
                only: manifest_checks # optional, defaults to running all checks
                output-file: results.json # optional, default does not save a results file
                output-format: json # optional, one of: csv, json, junit, sarif, tap. Defaults to json
                output-only-failures: false # optional, defaults to true
                send-pr-comment: true # optional, defaults to true
                show-all-failures: false # optional, defaults to false
                verbose: false # optional, defaults to false
```

We recommend pinning both a major and minor version number.

!!! tip "Trade-offs"
    **Best for:** enforcing conventions on every PR regardless of local developer setup — cannot be bypassed by individual contributors. Supports automated PR comments with violation details.

    **Watch out:** slower feedback loop than local hooks; consumes CI minutes for every push.

---

### Docker

Run `dbt-bouncer` via Docker:

```shell
docker run --rm \
    --volume "$PWD":/app \
    ghcr.io/godatadriven/dbt-bouncer:vX.X.X \
    --config-file /app/<PATH_TO_CONFIG_FILE>
```

!!! tip "Trade-offs"
    **Best for:** fully isolated, reproducible runs; no Python installation required on the host.

    **Watch out:** requires Docker; slightly heavier startup than a native install.

---

### Programmatic invocation

`dbt-bouncer` can be invoked programmatically. `run_bouncer` returns the exit code of the process.

```python
from pathlib import Path
from dbt_bouncer.main import run_bouncer

exit_code = run_bouncer(
    config_file=Path("path/to/dbt-bouncer.yml"),
    check="",  # optional, comma-separated check names to run
    dry_run=False,  # optional, print which checks would run without executing
    only="",  # optional, comma-separated categories e.g. "manifest_checks"
    output_file=Path("results.json"),  # optional
    output_format="json",  # optional, one of: "csv", "json", "junit", "sarif", "tap"
    output_only_failures=False,  # optional, only include failures in output
    show_all_failures=False,  # optional, print all failures to console
    verbosity=0,  # optional, increase for more detailed output
)
```

!!! tip "Trade-offs"
    **Best for:** embedding `dbt-bouncer` into existing Python tooling, test suites, or custom orchestration.

    **Watch out:** requires Python knowledge; not suitable for teams without Python experience.
