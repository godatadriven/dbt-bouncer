def pytest_configure(config):
    config.addinivalue_line("markers", "iterate_over_models: Tests that should run once per model")


import json
from pathlib import Path

import pytest
from dbt_artifacts_parser.parser import parse_manifest


@pytest.fixture(scope="session")
def manifest_obj():
    manifest_json_path = Path("dbt_project") / "target/manifest.json"
    with Path.open(manifest_json_path, "r") as fp:
        manifest_obj = parse_manifest(manifest=json.load(fp))
    return manifest_obj
