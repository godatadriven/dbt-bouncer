import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from dbt_bouncer.main import cli


def test_cli_coverage_non_json(caplog, tmp_path):
    # Config file
    bouncer_config = {
        "dbt_artifacts_dir": ".",
        "manifest_checks": [
            {
                "name": "check_model_directories",
                "include": "",
                "permitted_sub_directories": ["staging"],
            }
        ],
    }

    with Path(tmp_path / "dbt-bouncer.yml").open("w") as f:
        yaml.dump(bouncer_config, f)

    # Manifest file
    with Path.open(Path("./dbt_project/target/manifest.json"), "r") as f:
        manifest = json.load(f)

    with Path.open(tmp_path / "manifest.json", "w") as f:
        json.dump(manifest, f)

    # Run dbt-bouncer
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "--config-file",
            Path(tmp_path / "dbt-bouncer.yml").__str__(),
            "--output-file",
            tmp_path / "coverage.js",
        ],
    )

    assert type(result.exception) == RuntimeError
    assert (
        result.exception.args[0].find("Output file must have a `.json` extension. Got `.js`.") == 0
    )
    assert result.exit_code == 1
