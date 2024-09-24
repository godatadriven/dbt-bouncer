import json
from pathlib import Path

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "not_in_parallel: Unit tests that cannot be run in parallel."
    )


@pytest.fixture(scope="session")
def manifest_obj():
    from dbt_bouncer.parsers import DbtBouncerManifest, parse_manifest

    manifest_json_path = Path("dbt_project") / "target/manifest.json"
    with Path.open(manifest_json_path, "r") as fp:
        manifest_obj = parse_manifest(manifest=json.load(fp))
    return DbtBouncerManifest(**{"manifest": manifest_obj})
