from pathlib import Path

import orjson
import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "not_in_parallel: Unit test that cannot be run in parallel."
    )
    config.addinivalue_line(
        "markers", "not_in_parallel2: Unit test that cannot be run in parallel."
    )


@pytest.fixture(scope="session")
def manifest_obj():
    from dbt_bouncer.artifact_parsers.parsers_manifest import (
        DbtBouncerManifest,
        parse_manifest,
    )

    manifest_json_path = Path("dbt_project") / "target/manifest.json"
    manifest_obj = parse_manifest(
        manifest=orjson.loads(manifest_json_path.read_bytes())
    )
    return DbtBouncerManifest(**{"manifest": manifest_obj})
