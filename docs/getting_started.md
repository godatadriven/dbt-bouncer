## How to run `dbt-bouncer`

1. Generate dbt artifacts by running a dbt command:

    - `dbt parse` to generate a `manifest.json` artifact.
    - `dbt docs generate` to generate a `catalog.json` artifact (necessary if you are using [catalog checks](./checks_catalog.md)).

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