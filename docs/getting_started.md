## How to run `dbt-bouncer`

1. Generate dbt artifacts by running a dbt command:

    * `dbt parse` to generate a `manifest.json` artifact (no database connection required!).
    * `dbt docs generate` to generate a `catalog.json` artifact (necessary if you are using catalog checks).
    * `dbt run` (or any other command that implies it e.g. `dbt build`) to generate a `run_results.json` artifact (necessary if you are using run results checks).

1. Create a config file (`dbt-bouncer.yml`, `dbt-bouncer.toml`, or a `[tool.dbt-bouncer]` section in `pyproject.toml`), details [here](./config_file.md). Alternatively, you can run `dbt-bouncer init` to generate a basic configuration file.

1. Run `dbt-bouncer` to validate that your conventions are being maintained.

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
Running dbt-bouncer (X.X.X)...
config_file=PosixPath('dbt-bouncer-example.yml')
config_file_source='COMMANDLINE'
Config file passed via command line: dbt-bouncer-example.yml
Loading config from /home/pslattery/repos/dbt-bouncer/dbt-bouncer-example.yml...
Loading config from dbt-bouncer-example.yml...
Loaded config from dbt-bouncer-example.yml...
conf={'dbt_artifacts_dir': 'dbt_project/target', 'catalog_checks': [{'name': 'check_column_name_complies_to_column_type', 'column_name_pattern': '^is_.*', 'exclude': '^staging', 'types': ['BOOLEAN']}]}
Validating conf...
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

### Running as an executable using [uv](https://github.com/astral-sh/uv)

Run `dbt-bouncer` as a standalone Python executable using `uv`:

```shell
uvx dbt-bouncer --config-file <PATH_TO_CONFIG_FILE>
```

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

### Docker

Run `dbt-bouncer` via Docker:

```shell
docker run --rm \
    --volume "$PWD":/app \
    ghcr.io/godatadriven/dbt-bouncer:vX.X.X \
    --config-file /app/<PATH_TO_CONFIG_FILE>
```

### Programmatic invocation

`dbt-bouncer` can be invoked programmatically. `run_bouncer` returns the exit code of the process.

```python
from pathlib import Path
from dbt_bouncer.main import run_bouncer

exit_code = run_bouncer(
    config_file=Path("path/to/dbt-bouncer.yml"),
    output_file=Path("results.json"),  # optional
    output_format="json",  # optional, one of: "csv", "json", "junit", "sarif", "tap". Defaults to "json"
)
```

## How to contribute a check to `dbt-bouncer`

See [Adding a new check](./CONTRIBUTING.md#adding-a-new-check).

## How to add a custom check to `dbt-bouncer`

In addition to the checks built into `dbt-bouncer`, the ability to add custom checks is supported. This allows users to write checks that are specific to the conventions of their projects. To add a custom check:

1. Create an empty directory and add a `custom_checks_dir` key to your config file. The value of this key should be the path to the directory you just created, relative to where the config file is located.
1. In this directory create an empty `__init__.py` file.
1. In this directory create a subdirectory named `catalog`, `manifest` or `run_results` depending on the type of artifact you want to check.
1. In this subdirectory create a python file that defines a check. The check must meet the following criteria:

            * The function name must start with `check_`.
            * The function must be decorated with `@check` from `dbt_bouncer.check_decorator`.
            * The first positional parameter determines the resource type (e.g. `model`, `source`, `exposure`).
            * Use `fail()` from `dbt_bouncer.check_decorator` to signal a check failure.
            * Have a doc string that includes a description of the check, a list of possible input parameters and at least one example.
            * A clear message passed to `fail()` in the event of a failure.

1. In your config file, add the name of the check and any desired arguments.
1. Run `dbt-bouncer`, your custom check will be executed.

An example:

* Directory tree:

    ```shell
    .
    ├── dbt-bouncer.yml
    ├── dbt_project.yml
    ├── my_custom_checks
    |   ├── __init__.py
    |   └── manifest
    |       └── check_custom_to_me.py
    └── target
        └── manifest.json
    ```

* Contents of `check_custom_to_me.py`:

    ```python
    from dbt_bouncer.check_decorator import check, fail


    @check
    def check_model_deprecation_date(model):
        """Models cannot have a deprecation date in the past.

        Receives:
            model (ModelNode): The ModelNode object to check.

        Other Parameters:
            description (str | None): Description of what the check does.
            exclude (str | None): Regex pattern to exclude model paths.
            include (str | None): Regex pattern to include model paths.
            severity (Literal["error", "warn"] | None): Severity level. Default: `error`.

        Example(s):
            ```yaml
            manifest_checks:
                - name: check_model_deprecation_date
            ```
        """
        if model.deprecation_date is not None:
            fail(f"`{model.name}` has a deprecation_date set.")
    ```

* Contents of `dbt-bouncer.yml`:

    ```yaml
    custom_checks_dir: my_custom_checks

    manifest_checks:
        - name: check_model_deprecation_date
          include: ^models/staging/legacy_erp
    ```
