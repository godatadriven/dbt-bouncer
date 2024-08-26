## How to run `dbt-bouncer`

1. Generate dbt artifacts by running a dbt command:

    - `dbt parse` to generate a `manifest.json` artifact.
    - `dbt docs generate` to generate a `catalog.json` artifact (necessary if you are using [catalog checks](./checks/checks_catalog.md)).
    - `dbt run` (or any other command that implies it e.g. `dbt build`) to generate a `run_results.json` artifact (necessary if you are using [run results checks](./checks/checks_run_results.md)).

1. Create a `dbt-bouncer.yml` config file, details [here](./config_file.md).

1. Run `dbt-bouncer` to validate that your conventions are being maintained.

### Python

Install from [pypi.org](https://pypi.org/p/dbt-bouncer):

```shell
pip install dbt-bouncer
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

### GitHub Actions

Run `dbt-bouncer` as part of your CI pipeline:
```yaml
steps:
    ...

    - uses: godatadriven/dbt-bouncer@vX.X
      with:
        config-file: ./<PATH_TO_CONFIG_FILE>
        output-file: results.json # optional, default does not save a results file
        send-pr-comment: true # optional, defaults to true
        verbose: false # optional, defaults to false

    ...
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

### Pex

You can also run the `.pex` ([Python EXecutable](https://docs.pex-tool.org/whatispex.html#whatispex)) artifact directly once you have a python executable (3.8 -> 3.12) installed:

```bash
wget https://github.com/godatadriven/dbt-bouncer/releases/download/vX.X.X/dbt-bouncer.pex -O dbt-bouncer.pex

python dbt-bouncer.pex --config-file $PWD/<PATH_TO_CONFIG_FILE>
```
